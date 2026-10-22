
![Feast_Logo](https://raw.githubusercontent.com/feast-dev/feast/master/docs/assets/feast_logo.png)

# Feast Credit Risk Classification End-to-End Example

This example starts with an [OpenML](https://openml.org) credit risk dataset, and walks through the steps of preparing the data, setting up feature store resources, and serving features; this is all done inside the paradigm of an ML workflow, with the goal of helping users understand how Feast fits in the progression from data preparation, to model training and model serving.

The example is organized in five notebooks:
1. [01_Credit_Risk_Data_Prep.ipynb](01_Credit_Risk_Data_Prep.ipynb)
2. [02_Deploying_the_Feature_Store.ipynb](02_Deploying_the_Feature_Store.ipynb)
3. [03_Credit_Risk_Model_Training.ipynb](03_Credit_Risk_Model_Training.ipynb)
4. [04_Credit_Risk_Model_Serving.ipynb](04_Credit_Risk_Model_Serving.ipynb)
5. [05_Credit_Risk_Cleanup.ipynb](05_Credit_Risk_Cleanup.ipynb)

Run the notebooks in order to progress through the example. See below for prerequisite setup steps.

### Preparing your Environment
To run the example, install the Python dependencies. You may wish to do so inside a virtual environment. Open a command terminal, and run the following:

```
# create venv-example virtual environment
python -m venv venv-example
# activate environment
source venv-example/bin/activate
```

Install the Python dependencies:
```
pip install -r requirements.txt
```

Note that this example was tested with Python 3.11, but it should also work with other similar versions.

### Running the Notebooks
Once you have installed the Python dependencies, you can run the example notebooks. To run the notebooks locally, execute the following command in a terminal window:

```jupyter notebook```

You should see a browser window open a page where you can navigate to the example notebook (.ipynb) files and open them.
