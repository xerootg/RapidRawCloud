# rapidraw-cloud (Helm chart)

Deploys the RapidRawCloud server side: the **pairing** service and the fleet
**worker**. Both talk to a Garage/S3 **admin bucket** (`users/<username>/config.json`);
per-user library credentials live inside those config docs, not in this chart.

Provision the Garage bucket + keys first — see
[`docs/CLOUD_SETUP.md`](../../docs/CLOUD_SETUP.md).

## 1. Create the Secrets (not templated)

```sh
# pairing: READ-WRITE admin key + the browser proxy secret (>= 16 random bytes;
# the edge injects it as X-Rrcloud-Proxy-Secret so the service can trust
# X-Authentik-Username — the pod refuses to start without it)
kubectl -n rapidraw create secret generic rapidraw-pairing-secret \
  --from-literal=admin-s3-access-key=GK... --from-literal=admin-s3-secret-key=... \
  --from-literal=browser-proxy-secret="$(openssl rand -hex 32)"
# worker: READ-ONLY admin key
kubectl -n rapidraw create secret generic rapidraw-worker-secret \
  --from-literal=admin-s3-access-key=GK... --from-literal=admin-s3-secret-key=...
```

## 2. Install

```sh
helm install rrc deploy/helm/rapidraw-cloud -n rapidraw --create-namespace
```

For the themissing.xyz fleet (Traefik IngressRoute + Authentik forward-auth).
`pairing.ingressRoute.browserProxySecret` must equal the Secret's
`browser-proxy-secret`; keep it in an encrypted values file (sops/helm-secrets),
not on the command line. The chart refuses to render the IngressRoute without
forward-auth or without that value:

```sh
helm install rrc deploy/helm/rapidraw-cloud -n rapidraw --create-namespace \
  --set pairing.ingressRoute.enabled=true \
  --set pairing.ingressRoute.forwardAuth.enabled=true \
  -f secrets.yaml   # pairing.ingressRoute.browserProxySecret: <same value as the Secret>
```

Toggle components with `pairing.enabled` / `worker.enabled`; see `values.yaml`.
