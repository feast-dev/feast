# Installing Feast on Kubernetes with PostgreSQL TLS Demo using feast operator

This repository contains a series of Jupyter Notebooks that guide you through setting up [Feast](https://feast.dev/) on a Kubernetes cluster. 

In this demo, Feast connects to a PostgreSQL database running in TLS mode, ensuring secure communication between services. Additionally, the example demonstrates how feast application references TLS certificates using Kubernetes volumes and volume mounts. While the focus is on mounting TLS certificates, you can also mount any other resources supported by Kubernetes volumes.

## Prerequisites

- A running Kubernetes cluster with sufficient resources.
- [Helm](https://helm.sh/) installed and configured.
- The [Feast Operator](https://docs.feast.dev/) for managing Feast deployments.
- Jupyter Notebook or JupyterLab to run the provided notebooks.
- Basic familiarity with Kubernetes, Helm, and TLS concepts.

## Notebook Overview

The following Jupyter Notebooks will walk you through the entire process:

1. **[01-Install-postgres-tls-using-helm.ipynb](./01-Install-postgres-tls-using-helm.ipynb)**  
   Installs PostgreSQL in TLS mode using a Helm chart.

2. **[02-Install-feast.ipynb](02-Install-feast.ipynb)**  
   Deploys Feast using the Feast Operator.

3. **[03-demo-feast.ipynb](./03-Demo.ipynb)**  
   Demonstrates the functionality of Feast by executing [feast-credit-score-local-tutorial](https://github.com/feast-dev/feast-credit-score-local-tutorial/tree/f43b44b245ae2632b582f14176392cfe31f98da9) the tutorial.

4. **[04-Uninstall.ipynb](./04-Uninstall.ipynb)**  
   Uninstalls Feast, the Feast Operator, and the PostgreSQL deployments set up in this demo.

## How to Run the Demo

1. **Clone the Repository**

   ```shell
   https://github.com/feast-dev/feast.git
   cd examples/operator-postgres-ssl-demo
   ```
2. Start Jupyter Notebook or JupyterLab from the repository root:

```shell
jupyter notebook
```
3. Execute the Notebooks
Run the notebooks in the order listed above. Each notebook contains step-by-step instructions and code to deploy, test, and eventually clean up the demo components.


## Troubleshooting
* **Cluster Resources:**
Verify that your Kubernetes cluster has adequate resources before starting the demo.

* **Logs & Diagnostics:**
If you encounter issues, check the logs for the PostgreSQL and Feast pods. This can help identify problems related to TLS configurations or resource constraints.