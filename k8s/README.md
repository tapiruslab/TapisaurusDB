# ☸️ TapisaurusDB Cloud-Native Kubernetes Deployment

Deploy a fault-tolerant, 3-node High-Availability TapisaurusDB cluster on Kubernetes (EKS, GKE, AKS, or on-premise K8s).

---

## 🚀 Quick Deployment (3 Steps)

### 1. Create Secrets for Master Admin & JWT
```bash
kubectl create secret generic tapisaurus-credentials \
  --from-literal=admin-user=admin \
  --from-literal=admin-password=tapisaurus-super-secret-2026 \
  --from-literal=jwt-secret=tapisaurus-master-jwt-key-256bit
```

### 2. Apply the StatefulSet & Services
```bash
kubectl apply -f k8s/statefulset.yaml
```

### 3. Verify Cluster Health
```bash
# Check running pods
kubectl get pods -l app=tapisaurus

# Port forward Studio & HTTP API to local machine
kubectl port-forward svc/tapisaurus-service 27018:27018 27017:27017
```

Connect your application via `mongodb://tapisaurus-service.default.svc.cluster.local:27017` or `http://tapisaurus-service.default.svc.cluster.local:27018`.
