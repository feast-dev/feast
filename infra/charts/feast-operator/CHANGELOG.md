# Changelog

All notable changes to the Feast Operator Helm chart will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.52.0] - 2025-08-22

### Added
- Initial release of Feast Operator Helm chart
- Support for Feast v0.52.0
- Configurable deployment with customizable resources, replicas, and security contexts
- Full RBAC support with minimal required permissions
- Metrics service for monitoring and observability
- CRD installation with conditional deployment
- Namespace management with automatic creation
- Health checks and probes configuration
- Node affinity, tolerations, and selectors support
- Comprehensive documentation and examples
- ArtifactHub.io integration with rich metadata

### Features
- **Security**: Pod and container security contexts with non-root execution
- **Monitoring**: Built-in metrics endpoint on port 8443
- **Flexibility**: Configurable image registry, tags, and pull policies
- **Production Ready**: Resource limits, health checks, and graceful shutdown
- **Extensibility**: Support for custom labels, annotations, and environment variables

### Configuration Options
- Operator image configuration (repository, tag, pullPolicy)
- Resource requests and limits
- Replica count and scaling options
- Health check intervals and timeouts
- RBAC and service account settings
- Metrics service configuration
- Namespace and CRD management
- Security contexts and pod security policies

### Documentation
- Comprehensive README with installation instructions
- Configuration reference table
- Examples for common use cases
- Troubleshooting guide
- ArtifactHub metadata and annotations
