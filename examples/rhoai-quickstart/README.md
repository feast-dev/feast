# Quickstart: Running Feast example

This quickstart guide will walk you through setting up and using [Feast](https://feast.dev) as a feature store on Red Hat OpenShift AI. By the end of this tutorial, you’ll have the environment configured, sample data loaded, and features retrieved using Feast objects.

## Prerequisites
This example uses Jupyter Notebook to showcase Feast's capabilities. You'll need an environment where Jupyter Notebook can be executed.

You have two options for setting up the runtime environment:
1. **Running Jupyter on your local machine**: If you're new to Jupyter, refer to the official documentation [here](https://docs.jupyter.org/en/latest/running.html).
2. **Running Jupyter on Red Hat OpenShift AI (RHOAI)**: If you'd prefer to run Jupyter on the RHOAI platform, follow the instructions below.

## Using the Red Hat OpenShift AI (RHOAI) Platform
You can execute the Jupyter notebook directly on the RHOAI platform. If you don't have an existing RHOAI cluster, you can try this Feast example in the developer sandbox.

To get started, visit the [Red Hat OpenShift AI sandbox](https://www.redhat.com/en/technologies/cloud-computing/openshift/openshift-ai) and launch your environment.

### Getting Started with RHOAI
Before proceeding, it's helpful to familiarize yourself with Red Hat OpenShift AI. If you're new to the platform, check out this [short introductory video](https://youtu.be/75WtOSpn5qU?si=uT1xZfpuJBkVP7ha) for a quick overview of its features.

For detailed documentation on RHOAI, including how to work on data science projects, refer to the official product documentation [here](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_cloud_service/1/html/working_on_data_science_projects/using-data-science-projects_projects).

### Steps to Set Up a Workbench on the RHOAI Sandbox
Follow these brief steps to create a workbench on the RHOAI sandbox:

1. Navigate to your project (namespace) → **Dev**.
2. Go to the **Workbenches** tab.
3. Select **Create Workbench** and provide the necessary details.
4. Click **Create**.

Once your workbench is set up, you can import and run the Feast example using one of these methods:
1. **Clone the GitHub repository**: Clone the repo into your RHOAI workbench and run the Jupyter notebook.
2. **Upload files**: Upload the necessary files to your existing workbench and execute the notebook.

## Notebook Overview
The [feast-demo-quickstart.ipynb](feast-demo-quickstart.ipynb) notebook will guide you through:

This notebook will use Driver entity (or model) to demonstrate the feast functionalities. 
You should be able to execute the same jupyter notebook in a standalone environment as well.

- **Setting up the Feast repository**: Load sample driver data, generate training datasets, run offline inference, and ingest batch features into the online store. You'll also learn how to fetch features for inference using Feast’s `FeatureView` and `FeatureService`.

- **Configuring a Remote Online Topology**: Set up a remote online server and client, and retrieve features in real-time using the remote online client.

- **Configuring a Remote Registry Topology**: Set up a remote registry server and client, and retrieve Feast metadata using the remote registry client.

