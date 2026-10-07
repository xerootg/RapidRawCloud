# rapidraw-cloud (Helm chart)

Deploys the RapidRawCloud server side: the **pairing** service and the fleet
**worker**. Both talk to a Garage/S3 **admin bucket** (`users/<username>/config.json`);
per-user library credentials live inside those config docs, not in this chart.

Provision the Garage bucket + keys first — see
[`docs/CLOUD_SETUP.md`](../../docs/CLOUD_SETUP.md).

## 1. Create the Secrets (not templated)

```sh
# pairing: READ-WRITE admin key
kubectl -n rapidraw create secret generic rapidraw-pairing-secret \
  --from-literal=admin-s3-access-key=GK... --from-literal=admin-s3-secret-key=...
# worker: READ-ONLY admin key
kubectl -n rapidraw create secret generic rapidraw-worker-secret \
  --from-literal=admin-s3-access-key=GK... --from-literal=admin-s3-secret-key=...
```

## 2. Install

```sh
helm install rrc deploy/helm/rapidraw-cloud -n rapidraw --create-namespace
```

For the themissing.xyz fleet (Traefik IngressRoute + Authentik forward-auth):

```sh
helm install rrc deploy/helm/rapidraw-cloud -n rapidraw --create-namespace \
  --set pairing.ingressRoute.enabled=true \
  --set pairing.ingressRoute.forwardAuth.enabled=true
```

Toggle components with `pairing.enabled` / `worker.enabled`; see `values.yaml`.
