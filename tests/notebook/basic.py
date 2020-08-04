from .notebook import run_notebook, get_notebook_from_gcs
import pytest


@pytest.mark.usefixtures("output_bucket_name", "test_run_id")
def test_basic(output_bucket_name: str, test_run_id: str):
    notebook_path = '/home/jovyan/basic/basic.ipynb'
    notebook_blob = f"%s/basic.ipynb" % test_run_id
    output_notebook_uri = f"gs://%s/%s" % (output_bucket_name, notebook_blob)

    # Run Jupyter notebook
    run_notebook(notebook_path, output_notebook_uri)

    # Get notebook from GCS
    notebook_output = get_notebook_from_gcs(output_bucket_name, notebook_blob)

    # Find historical serving results
    historical_serving_output_lines = None
    cells = notebook_output["cells"]
    for cell in cells:
        if cell["source"][0] == "print(df.head(50))":
            historical_serving_output_lines = cell["outputs"][0]["text"]

    assert historical_serving_output_lines is not None

    # Assert that no "None" values are returned from historical serving
    print(historical_serving_output_lines)
    for line in historical_serving_output_lines:
        if "None" in line:
            raise Exception(
                'Found "None" type in response line from historical serving:\nLine: %s\nNotebook: %s'
                % (line, output_notebook_uri)
            )
