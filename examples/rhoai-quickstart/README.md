# Quickstart: Running Feast on [Red Hat OpenShift AI](https://www.redhat.com/en/technologies/cloud-computing/openshift/openshift-ai)

This quickstart demonstrates how to set up and use [Feast](https://feast.dev) as a feature store on Red Hat OpenShift AI. By following these steps, you'll set up the environment, load sample data, and retrieve features using various Feast objects.

## Prerequisites

Before you begin, ensure you are familiar with Red Hat OpenShift AI. If you're new to the platform, watch this [short overview video](https://youtu.be/75WtOSpn5qU?si=uT1xZfpuJBkVP7ha) to get started.

## Notebook Overview

The [feast-demo-quickstart.ipynb](feast-demo-quickstart.ipynb) notebook will guide you through:

This notebook will use Driver entity (or model) to demonstrate the feast functionalities. 
You should be able to execute the same jupyter notebook in a standalone environment as well.

- **Setting up the Feast repository**: Load sample driver data, generate training datasets, run offline inference, and ingest batch features into the online store. You'll also learn how to fetch features for inference using Feast’s `FeatureView` and `FeatureService`.

- **Configuring a Remote Online Topology**: Set up a remote online server and client, and retrieve features in real-time using the remote online client.

- **Configuring a Remote Registry Topology**: Set up a remote registry server and client, and retrieve Feast metadata using the remote registry client.

