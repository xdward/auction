# Auction

Go service that runs an auction workflow over a gRPC server.

```mermaid
flowchart RL
    grpc@{ shape: rect, label: "gRPC Server" }
    request@{ shape: stadium, label: "Request" }
    reply@{ shape: stadium, label: "Reply" }
    redis@{ shape: cyl, label: "Redis"}
    sell@{ shape: das, label: "Sell" }
    bid@{ shape: das, label: "Bid" }
    cancel@{ shape: das, label: "Cancel" }
    schedules@{ shape: processes, label: "Schedules" }

    grpc ===|Sell| sell
    grpc ===|Bid| bid
    grpc ===|Cancel| cancel

    subgraph NATS
        direction LR

        request --> sell
        request --> bid
        request --> cancel

        sell --> c1((Sub))
        bid --> c2((Sub))
        cancel --> c3((Sub))

        c1 .->|Deadline| schedules

        c1 --> reply
        c2 --> reply
        c3 --> reply

        schedules -->|Expired| c4((C))
    end

    c1 .->|Updates| redis
    redis -.-|EventStream| grpc
```

See the [contracts](https://github.com/xdward/auction-contracts) repository for service definitions.

## Features

- Requests are queued through NATS for low latency and high throughput
- Auction state is stored in Redis for persistency and fast read/writes
- Atomic operations through Redis transactions for safe concurrent updates
- Snapshots and real-time updates for client synchronization

## Local Development

Run the command below to start the containers:

```sh
docker compose -p auction up -d --build
```

To stop the containers:

```sh
docker compose stop
```

## Kubernetes

The chart found under the [helm](helm/auction) directory can be used to help
setup and deploy the auction service in Kubernetes. This should be done after
setting up a NATS cluster and having a managed Redis instance available. Follow
the steps below to deploy the auction service on your k8s cluster.

### Containers

Build and load the application images first:

```sh
docker build --target server -t auction/server:latest .
docker build --target worker -t auction/worker:latest .
kind load docker-image auction/server:latest auction/worker:latest
```

### Adding NATS to K8S

In the `nats` namespace, install a NATS cluster with JetStream enabled. The
configuration below provides 1GB for each stream. For further options, see the
[NATS Helm Chart documentation](https://github.com/nats-io/k8s/tree/main/helm/charts/nats).

```sh
kubectl create namespace nats
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm repo update
helm upgrade --install nats nats/nats --namespace nats \
  --set container.image.tag=2.15-alpine \
  --set config.cluster.enabled=true \
  --set config.cluster.replicas=3 \
  --set config.jetstream.enabled=true \
  --set config.jetstream.fileStore.pvc.size=1Gi

kubectl get pods -n nats
```

### Managing JetStream

In the `auction` namespace, install the [NACK](https://github.com/nats-io/nack)
controller for managing the JetStream. The auction service uses it for
configuring the `Schedule` stream and its consumers.

```sh
kubectl create namespace auction
helm upgrade --install auction-nack nats/nack --namespace auction \
  --set jetstream.nats.url=nats://nats.nats.svc.cluster.local:4222 \
  --set jetstream.controlLoop=true \
  --set namespaced=true
```

### Installing the Auction Service

Use the command below to install the `auction` service. The `nats.url`
configuration corresponds to the cluster we set up previously -- `redis.url`
uses a placeholder.

```sh
helm upgrade --install auction ./helm/auction --namespace auction \
  --set redis.url="redis://$REDIS_ADDRESS" \
  --set nats.url="nats://nats.nats.svc.cluster.local:4222"

kubectl get stream,consumer --namespace auction
kubectl get pods --namespace auction
```

> [!TIP]
> For local Kind development, run Redis on the Kind Docker network:
>
> ```sh
> docker run -d --name auction-redis --network kind redis:8.10-alpine
> REDIS_ADDRESS="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' auction-redis):6379"
> ```

To access the gRPC server from your workspace, forward the Kubernetes service to a local port:

```sh
kubectl port-forward service/auction-grpc 50051:50051 --namespace auction
```

### Clean up

Stop the port-forward with `Ctrl+C`, then remove the Helm releases and namespaces:

```sh
helm uninstall auction -n auction
helm uninstall auction-nack -n auction
helm uninstall nats -n nats

kubectl delete namespace auction
kubectl delete namespace nats
kubectl delete crd streams.jetstream.nats.io consumers.jetstream.nats.io
```

If you used a local Redis container, remove it with:

```sh
docker rm -f auction-redis
```
