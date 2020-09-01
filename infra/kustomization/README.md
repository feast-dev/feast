feast-kustomization contains open source feast-core kustomization and will contain feast-serving kustomization

├── README.md
├── base
│   ├── feast-core
│   │   ├── application.yml
│   │   ├── deployment.yaml
│   │   ├── kustomization.yaml
│   │   └── service.yaml
│   ├── kafka
│   │   └── kustomization.yaml
│   ├── kafka-rest-proxy
│   │   ├── 10-schema-registry-deployment.yaml
│   │   ├── 11-schema-registry-service.yaml
│   │   ├── 20-rest-proxy.yaml
│   │   ├── 21-rest-service.yaml
│   │   └── kustomization.yaml
│   ├── postgres
│   │   ├── kustomization.yaml
│   │   ├── postgres.yaml
│   │   ├── secret.yaml
│   │   └── service.yaml
│   └── redis
│       ├── kustomization.yaml
│       ├── redis.yaml
│       └── service.yaml
└── sample
    ├── feast-core
    │   ├── application-test.yml
    │   ├── deployment.yaml
    │   ├── ksa.yaml
    │   └── kustomization.yaml
    ├── kafka
    │   ├── kafka-scale1.json
    │   ├── kafka.yaml
    │   ├── kustomization.yaml
    │   ├── outsider-0.yaml
    │   └── zookeeper.yaml
    ├── kafka-rest-proxy
    │   ├── kustomization.yaml
    │   └── rest-proxy-service.yaml
    └── postgres
        ├── kustomization.yaml
        ├── pd.yaml
        └── volume-claims.yaml

Resource Config

|         | Deployed to cluster            | Purpose            |  
|---------|--------------------------------|--------------------|------------------
|base     | No                             | Shared config      |
|sample   | Yes - Manually or Continuously | Deployable config  | feast-core: feast-core deployable config 
