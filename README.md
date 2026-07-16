# Auction

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
