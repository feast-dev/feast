from .notebook import run_notebook
import pytest


@pytest.mark.usefixtures("output_bucket_name", "test_run_id")
def test_churn_prediction(output_bucket_name: str, test_run_id: str):
    notebook_path = '/home/jovyan/churn_prediction/churn_prediction.ipynb'
    notebook_blob = f"%s/churn_prediction.ipynb" % test_run_id
    output_notebook_uri = f"gs://%s/%s" % (output_bucket_name, notebook_blob)

    # Run Jupyter notebook
    run_notebook(notebook_path, output_notebook_uri)

    # # TODO: Add validation steps for notebook output data
