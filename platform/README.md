# ChillFlow Platform Infrastructure

Shared infrastructure components for the ChillFlow data pipeline platform.

## Structure

```
platform/
├── k8s/                    # Kubernetes manifests
│   ├── base/              # Base components (Postgres, Redis, Kafka, Monitoring)
│   └── overlays/          # Environment-specific configurations
│       ├── dev/           # Development environment
│       ├── staging/       # Staging environment
│       └── prod/          # Production environment
├── terraform/             # Cloud infrastructure
│   ├── envs/              # Environment-specific configurations
│   │   ├── dev/           # Development infrastructure
│   │   ├── staging/       # Staging infrastructure
│   │   └── prod/          # Production infrastructure
│   └── modules/           # Reusable Terraform modules
├── compose/               # Local development
│   ├── docker-compose.yml # Local infrastructure stack
│   └── .env.example       # Environment variables template
└── ops/                   # Operations and monitoring
    ├── runbooks/          # Operational runbooks
    ├── dashboards/         # Grafana dashboard definitions
    └── alerts/            # Alerting rules
```

## Local Development

```bash
# Setup environment (first time only)
make setup-env

# Start infrastructure stack
make up

# Start with monitoring
make up-observability

# Check status
make status

# Stop infrastructure
make down
```

### Environment Configuration

The platform uses environment variables for configuration. Copy `platform/compose/env.example` to `platform/compose/.env` and customize as needed:

```bash
# Create .env file from template
make setup-env

# Edit configuration
vim platform/compose/.env
```

## Environment Strategy

- **Local**: Docker Compose for development
- **Staging**: Kubernetes with Kustomize overlays
- **Production**: Terraform + Kubernetes with production-grade configurations

## Components

- **PostgreSQL**: Primary database
- **Redis**: Caching and session storage
- **Kafka**: Message streaming
- **Grafana**: Monitoring dashboards
- **Prometheus**: Metrics collection
- **Ingress**: Traffic routing and SSL termination
