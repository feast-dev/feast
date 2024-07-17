
/install_feast.sh



# Feast Setup and Port Forwarding Instructions

This document provides instructions for installing Feast and setting up port forwarding for various services for Kubernetes cluster.

## Installation

To install Feast, follow these steps:

1. **Run the Installation Script:**

   ```sh
   ./install_feast.sh

For local testing the forward PostgreSQL Service and Feast Servers Port with below commands:
   ```sh
   kubectl port-forward svc/postgresql 5432:5432
   kubectl port-forward svc/feast-offline-server-feast-feature-server 8815:80
   kubectl port-forward svc/feast-registry-server-feast-feature-server 6570:80
   kubectl port-forward svc/feast-feature-server 6566:80