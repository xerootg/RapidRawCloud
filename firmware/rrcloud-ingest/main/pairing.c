#include "pairing.h"
#include "util.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "esp_log.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "cJSON.h"
#include "rrc_proto.h"
#include "rrc_sigv4.h"
#include "app_config.h"
#include "sync.h"
#include "log_ring.h"

static const char *TAG = "pair";
static pair_status_t st;
static SemaphoreHandle_t mtx;
static volatile bool cancel;
static bool running;
static char service_base[200];

#define LOCK() xSemaphoreTake(mtx, portMAX_DELAY)
#define UNLOCK() xSemaphoreGive(mtx)

static void set_state(pair_state_t s, const char *msg)
{
    LOCK();
    st.state = s;
    if (msg) scpy(st.message, sizeof st.message, msg);
    UNLOCK();
    if (msg) log_ring_printf("pairing: %s", msg);
}

/* Simple buffered HTTP request (GET or form POST) returning the body + status. */
static int http_fetch(const char *url, const char *method, const char *form_body, const char *bearer, char **body_out, size_t *len_out)
{
    esp_http_client_config_t cfg = {
        .url = url,
        .method = form_body ? HTTP_METHOD_POST : HTTP_METHOD_GET,
        .timeout_ms = 15000,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .buffer_size = 4096,
        .buffer_size_tx = 2048,
        .max_redirection_count = 3,
    };
    (void)method;
    esp_http_client_handle_t c = esp_http_client_init(&cfg);
    if (!c) return -1;
    if (form_body) esp_http_client_set_header(c, "Content-Type", "application/x-www-form-urlencoded");
    esp_http_client_set_header(c, "Accept", "application/json");
    char *auth = NULL;
    if (bearer) {
        size_t al = strlen(bearer) + 8;
        auth = malloc(al);
        if (!auth) { esp_http_client_cleanup(c); return -1; }
        snprintf(auth, al, "Bearer %s", bearer); /* JWTs are routinely > 1 KiB */
        esp_http_client_set_header(c, "Authorization", auth);
    }
    size_t blen = form_body ? strlen(form_body) : 0;
    int status = -1;
    if (esp_http_client_open(c, (int)blen) != ESP_OK) goto out;
    if (blen && esp_http_client_write(c, form_body, (int)blen) < 0) goto out;
    if (esp_http_client_fetch_headers(c) < 0) goto out;
    status = esp_http_client_get_status_code(c);
    size_t cap = 8192, len = 0;
    char *buf = malloc(cap);
    if (!buf) { status = -1; goto out; }
    for (;;) {
        if (len + 1024 > cap) { cap *= 2; if (cap > 65536) break; char *nb = realloc(buf, cap); if (!nb) break; buf = nb; }
        int r = esp_http_client_read(c, buf + len, (int)(cap - len - 1));
        if (r <= 0) break;
        len += (size_t)r;
    }
    buf[len] = 0;
    *body_out = buf; *len_out = len;
out:
    esp_http_client_close(c);
    esp_http_client_cleanup(c);
    free(auth);
    return status;
}

static char *json_str_dup(cJSON *o, const char *k)
{
    cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    return cJSON_IsString(v) ? strdup(v->valuestring) : NULL;
}

static void form_encode(const char *in, char *out, size_t cap) { rrc_sigv4_uri_encode(in, true, out, cap); }

static void pairing_task(void *arg)
{
    (void)arg;
    char *body = NULL; size_t blen = 0;
    char url[512];
    char *issuer = NULL, *client_id = NULL, *config_endpoint = NULL, *device_ep = NULL, *token_ep = NULL, *device_code = NULL;
    int interval = 5, expires = 600;

    set_state(PAIR_DISCOVERING, "contacting pairing service");
    snprintf(url, sizeof url, "%s/api/pairing-info", service_base);
    int status = http_fetch(url, "GET", NULL, NULL, &body, &blen);
    if (status != 200 || !body) { set_state(PAIR_FAILED, "pairing service unreachable (GET /api/pairing-info)"); goto done; }
    {
        cJSON *o = cJSON_ParseWithLength(body, blen);
        if (o) { issuer = json_str_dup(o, "issuer"); client_id = json_str_dup(o, "clientId"); config_endpoint = json_str_dup(o, "configEndpoint"); cJSON_Delete(o); }
        free(body); body = NULL;
    }
    if (!issuer || !client_id) { set_state(PAIR_FAILED, "bad pairing-info response"); goto done; }
    size_t il = strlen(issuer); while (il && issuer[il - 1] == '/') issuer[--il] = 0;
    snprintf(url, sizeof url, "%s/.well-known/openid-configuration", issuer);
    status = http_fetch(url, "GET", NULL, NULL, &body, &blen);
    if (status != 200 || !body) { set_state(PAIR_FAILED, "OIDC issuer unreachable"); goto done; }
    {
        cJSON *o = cJSON_ParseWithLength(body, blen);
        if (o) { device_ep = json_str_dup(o, "device_authorization_endpoint"); token_ep = json_str_dup(o, "token_endpoint"); cJSON_Delete(o); }
        free(body); body = NULL;
    }
    if (!token_ep) { set_state(PAIR_FAILED, "OIDC discovery has no token_endpoint"); goto done; }
    if (!device_ep) {
        /* Authentik publishes /application/o/device/ as the device endpoint. */
        const char *app = strstr(issuer, "/application/o/");
        if (app) { size_t base = (size_t)(app - issuer); device_ep = malloc(base + 32); if (device_ep) { memcpy(device_ep, issuer, base); strcpy(device_ep + base, "/application/o/device/"); } }
    }
    if (!device_ep) { set_state(PAIR_FAILED, "issuer offers no device_authorization_endpoint (enable a device-code flow on the Authentik brand)"); goto done; }

    /* 3. device authorization */
    {
        char cid[256]; form_encode(client_id, cid, sizeof cid);
        char form[400];
        snprintf(form, sizeof form, "client_id=%s&scope=openid%%20profile%%20email", cid);
        status = http_fetch(device_ep, "POST", form, NULL, &body, &blen);
        if (status != 200 || !body) { char m[160]; snprintf(m, sizeof m, "device authorization rejected (HTTP %d) — is device-code flow enabled for this brand?", status); set_state(PAIR_FAILED, m); goto done; }
        cJSON *o = cJSON_ParseWithLength(body, blen);
        if (!o) { set_state(PAIR_FAILED, "bad device authorization response"); goto done; }
        device_code = json_str_dup(o, "device_code");
        char *uc = json_str_dup(o, "user_code"), *vu = json_str_dup(o, "verification_uri"), *vuc = json_str_dup(o, "verification_uri_complete");
        cJSON *iv = cJSON_GetObjectItemCaseSensitive(o, "interval"); if (cJSON_IsNumber(iv) && iv->valueint > 0) interval = iv->valueint;
        cJSON *ex = cJSON_GetObjectItemCaseSensitive(o, "expires_in"); if (cJSON_IsNumber(ex) && ex->valueint > 0) expires = ex->valueint;
        LOCK();
        if (uc) scpy(st.user_code, sizeof st.user_code, uc);
        if (vu) scpy(st.verification_uri, sizeof st.verification_uri, vu);
        if (vuc) scpy(st.verification_uri_complete, sizeof st.verification_uri_complete, vuc);
        st.expires_in = expires;
        UNLOCK();
        free(uc); free(vu); free(vuc);
        cJSON_Delete(o);
        free(body); body = NULL;
        if (!device_code) { set_state(PAIR_FAILED, "no device_code in response"); goto done; }
    }
    set_state(PAIR_WAITING_USER, "open the verification link and approve this dock");

    /* 4. poll */
    char *access_token = NULL;
    {
        char cid[256], dc[700]; form_encode(client_id, cid, sizeof cid); form_encode(device_code, dc, sizeof dc);
        char form[1100];
        snprintf(form, sizeof form, "grant_type=urn%%3Aietf%%3Aparams%%3Aoauth%%3Agrant-type%%3Adevice_code&client_id=%s&device_code=%s", cid, dc);
        int waited = 0;
        while (!cancel && waited < expires) {
            vTaskDelay(pdMS_TO_TICKS(interval * 1000));
            waited += interval;
            status = http_fetch(token_ep, "POST", form, NULL, &body, &blen);
            if (!body) continue;
            cJSON *o = cJSON_ParseWithLength(body, blen);
            free(body); body = NULL;
            if (!o) continue;
            if (status == 200) { access_token = json_str_dup(o, "access_token"); cJSON_Delete(o); break; }
            char *err = json_str_dup(o, "error");
            cJSON_Delete(o);
            if (err && !strcmp(err, "authorization_pending")) { free(err); set_state(PAIR_POLLING, NULL); continue; }
            if (err && !strcmp(err, "slow_down")) { free(err); interval += 5; continue; }
            char m[160]; snprintf(m, sizeof m, "sign-in failed: %s", err ? err : "unknown error"); free(err);
            set_state(PAIR_FAILED, m); goto done;
        }
        if (!access_token) { set_state(PAIR_FAILED, cancel ? "cancelled" : "sign-in timed out"); goto done; }
    }

    /* 5. config */
    {
        const char *ep = config_endpoint && *config_endpoint ? config_endpoint : "/api/config";
        if (!strncmp(ep, "http://", 7) || !strncmp(ep, "https://", 8)) snprintf(url, sizeof url, "%s", ep); /* absolute URL */
        else snprintf(url, sizeof url, "%s%s%s", service_base, ep[0] == '/' ? "" : "/", ep);
        status = http_fetch(url, "GET", NULL, access_token, &body, &blen);
        memset(access_token, 0, strlen(access_token)); free(access_token);
        if (status == 404) { set_state(PAIR_FAILED, "no cloud config stored for your account yet — sign in to the pairing site in a browser and add your bucket first"); goto done; }
        if (status != 200 || !body) { char m[120]; snprintf(m, sizeof m, "config fetch rejected (HTTP %d)", status); set_state(PAIR_FAILED, m); goto done; }
        cJSON *o = cJSON_ParseWithLength(body, blen);
        free(body); body = NULL;
        cJSON *sync = o ? cJSON_GetObjectItemCaseSensitive(o, "sync") : NULL;
        cJSON *creds = o ? cJSON_GetObjectItemCaseSensitive(o, "credentials") : NULL;
        if (!sync || !creds) { if (o) cJSON_Delete(o); set_state(PAIR_FAILED, "bad config document"); goto done; }
        app_config_t c = *app_config_get();
        char *endpoint = json_str_dup(sync, "endpoint"), *bucket = json_str_dup(sync, "bucket"), *region = json_str_dup(sync, "region");
        char *ak = json_str_dup(creds, "accessKeyId"), *sk = json_str_dup(creds, "secretAccessKey");
        if (!endpoint || !bucket || !ak || !sk) { set_state(PAIR_FAILED, "config document missing endpoint/bucket/credentials"); }
        else {
            scpy(c.s3_endpoint, sizeof c.s3_endpoint, endpoint);
            scpy(c.s3_bucket, sizeof c.s3_bucket, bucket);
            scpy(c.s3_region, sizeof c.s3_region, region && *region ? region : "garage");
            scpy(c.s3_access_key, sizeof c.s3_access_key, ak);
            scpy(c.pairing_url, sizeof c.pairing_url, service_base);
            app_config_set_secret(sk);
            app_config_save(&c);
            sync_config_changed();
            char m[200]; snprintf(m, sizeof m, "paired: %s / %s", endpoint, bucket);
            set_state(PAIR_DONE, m);
        }
        if (sk) memset(sk, 0, strlen(sk));
        free(endpoint); free(bucket); free(region); free(ak); free(sk);
        cJSON_Delete(o);
    }
done:
    free(body); free(issuer); free(client_id); free(config_endpoint); free(device_ep); free(token_ep); free(device_code);
    running = false;
    vTaskDelete(NULL);
}

esp_err_t pairing_begin(const char *service_url)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    if (running) return ESP_ERR_INVALID_STATE;
    if (!service_url || !*service_url) return ESP_ERR_INVALID_ARG;
    const char *u = service_url;
    while (*u == ' ') u++;
    if (!strncmp(u, "http://", 7) || !strncmp(u, "https://", 8)) scpy(service_base, sizeof service_base, u);
    else snprintf(service_base, sizeof service_base, "https://%s", u);
    size_t n = strlen(service_base);
    while (n && (service_base[n - 1] == '/' || service_base[n - 1] == ' ')) service_base[--n] = 0;
    LOCK(); memset(&st, 0, sizeof st); UNLOCK();
    cancel = false;
    running = true;
    if (xTaskCreate(pairing_task, "pairing", 12288, NULL, 3, NULL) != pdPASS) { running = false; return ESP_ERR_NO_MEM; }
    ESP_LOGI(TAG, "pairing with %s", service_base);
    return ESP_OK;
}

void pairing_cancel(void) { cancel = true; }

void pairing_get_status(pair_status_t *out)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    LOCK(); *out = st; UNLOCK();
}

int pairing_status_json(char *out, size_t cap)
{
    pair_status_t s;
    pairing_get_status(&s);
    static const char *names[] = {"idle", "discovering", "waiting_user", "polling", "done", "failed"};
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "state", true); rrc_jsonw_str(&w, names[s.state]);
    rrc_jsonw_key(&w, "user_code", false); rrc_jsonw_str(&w, s.user_code);
    rrc_jsonw_key(&w, "verification_uri", false); rrc_jsonw_str(&w, s.verification_uri);
    rrc_jsonw_key(&w, "verification_uri_complete", false); rrc_jsonw_str(&w, s.verification_uri_complete);
    rrc_jsonw_key(&w, "expires_in", false); rrc_jsonw_i64(&w, s.expires_in);
    rrc_jsonw_key(&w, "message", false); rrc_jsonw_str(&w, s.message);
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}
