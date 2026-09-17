from kfp import local
from kfp import dsl

local.init(runner=local.DockerRunner())

# Define the base image to be used by all components
PYTHON_BASE_IMAGE = "python:3.10"

@dsl.component(base_image=PYTHON_BASE_IMAGE, packages_to_install=["requests", "pandas", "pyarrow"])
def import_test_pdfs(output_path: dsl.OutputPath("Directory")):
    import io
    import pathlib
    import requests
    import pandas as pd

    BASE_URL = 'https://raw.githubusercontent.com/DS4SD/docling/refs/heads/main/tests/data/pdf/'
    PDF_FILES = [
        '2203.01017v2.pdf',
        # '2305.03393v1-pg9.pdf', '2305.03393v1.pdf',
        # 'amt_handbook_sample.pdf', 'code_and_formula.pdf', 'picture_classification.pdf',
        # 'redp5110_sampled.pdf', 'right_to_left_01.pdf', 'right_to_left_02.pdf', 'right_to_left_03.pdf'
    ]
    # INPUT_DOC_PATHS = [os.path.join(BASE_URL, pdf_file) for pdf_file in PDF_FILES]

    # Download PDF files
    output_dict = {}
    for file_name in PDF_FILES:
        try:
            r = requests.get(BASE_URL + file_name)
            pdf_bytes = io.BytesIO(r.content)
            output_dict[file_name] = pdf_bytes.getvalue()
        except Exception as e:
            print(f"Error with {file_name} \n{e}")

    # Write the downloaded PDFs to the output path
    output_path = pathlib.Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    output_dict = pd.DataFrame.from_dict(output_dict, orient='index', columns=['bytes']).reset_index()
    output_dict.rename({"index": "file_name"}, axis=1, inplace=True)
    output_dict['file_name'] = output_dict['file_name'].str.replace('.pdf', '')

    # Save PDFs data to the output path
    file_name_and_path = str(output_path / "pdfs.parquet")
    print(f"exporting data to {file_name_and_path}")
    output_dict.to_parquet(file_name_and_path)


@dsl.component(base_image=PYTHON_BASE_IMAGE, packages_to_install=["transformers", "sentence-transformers", "docling", "pandas", "pyarrow"])
def process_pdfs(input_path: dsl.InputPath("Directory"), output_path: dsl.OutputPath("Directory")):
    import os
    import logging
    import pandas as pd
    from pathlib import Path
    from transformers import AutoTokenizer
    from sentence_transformers import SentenceTransformer
    from docling.chunking import HybridChunker
    from docling.datamodel.base_models import ConversionStatus, InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    # Logger setup
    logging.basicConfig(level=logging.INFO)
    _log = logging.getLogger(__name__)
    Path(output_path).mkdir(parents=True, exist_ok=True)

    BASE_URL = 'https://raw.githubusercontent.com/DS4SD/docling/refs/heads/main/tests/data/pdf/'

    # Load tokenizer and embedding model
    EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
    MAX_TOKENS = 64  # Small token limit for demonstration
    tokenizer = AutoTokenizer.from_pretrained(EMBED_MODEL_ID)
    embedding_model = SentenceTransformer(EMBED_MODEL_ID)
    chunker = HybridChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS, merge_peers=True)

    def embed_text(text: str) -> list[float]:
        """Generate an embedding for a given text."""
        return embedding_model.encode([text], normalize_embeddings=True).tolist()[0]

    # Read input PDFs data
    pdfs_df = pd.read_parquet(input_path + "/pdfs.parquet")
    INPUT_DOC_PATHS = [BASE_URL + url + ".pdf" for url in pdfs_df['file_name'].tolist()]

    # Configure PDF processing
    pipeline_options = PdfPipelineOptions()
    pipeline_options.generate_page_images = True

    doc_converter = DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )

    # Perform the document conversion and chunking
    print("running doc_converter...")
    conv_results = doc_converter.convert_all(INPUT_DOC_PATHS, raises_on_error=False)

    rows = []
    for i, conv_res in enumerate(conv_results):
        if conv_res.status != ConversionStatus.SUCCESS:
            continue

        file_name = conv_res.input.file.stem
        document = conv_res.document
        document_markdown = document.export_to_markdown() if document else ""

        print(f"embedding chunk {i}...")
        for chunk in chunker.chunk(dl_doc=document):
            raw_chunk = chunker.serialize(chunk)
            embedding = embed_text(raw_chunk)
            rows.append({
                "file_name": file_name,
                "full_document_markdown": document_markdown,
                "raw_chunk_markdown": raw_chunk,
                "chunk_embedding": embedding,
            })

    # Save processed data to output path
    df = pd.DataFrame(rows)
    df.to_parquet(output_path + "/processed_data.parquet")


@dsl.component(base_image=PYTHON_BASE_IMAGE, packages_to_install=["feast"])
def store_in_feast(input_path: dsl.InputPath("Parquet"), output_path: dsl.OutputPath("Directory")):
    import os
    import pandas as pd
    import requests
    import json
    from typing import Dict, List, Any, Optional

    class FeastClient:
        def __init__(self, feature_server_url: str):
            """
            Initializes the FeastClient with the URL of the Feast feature server.

            Args:
                feature_server_url (str): The base URL of the Feast feature server.
            """
            self.feature_server_url = feature_server_url

        def get_online_features(
                self,
                entities: Dict[str, List[Any]],
                feature_service: Optional[str] = None,
                features: Optional[List[str]] = None,
                full_feature_names: bool = False,
                query_embedding: Optional[List[float]] = None,
                query_string: Optional[str] = None,
        ) -> Dict[str, Any]:
            """
            Retrieves online features from the Feast feature server.

            Args:
                entities (Dict[str, List[Any]]): A dictionary of entity names to lists of entity values.
                feature_service (Optional[str]): The name of the feature service to use.
                features (Optional[List[str]]): A list of feature names to retrieve.
                full_feature_names (bool): Whether to return full feature names.
                query_embedding (Optional[List[float]]): Query embedding for document retrieval.
                query_string (Optional[str]): Query string for document retrieval.

            Returns:
                Dict[str, Any]: A dictionary containing the online features.
            """
            url = f"{self.feature_server_url}/get-online-features"
            headers = {"Content-Type": "application/json", "accept": "application/json"}
            payload = {
                "entities": entities,
                "feature_service": feature_service,
                "features": features,
                "full_feature_names": full_feature_names,
                "query_embedding": query_embedding,
                "query_string": query_string,
            }
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()

        def retrieve_online_documents(
                self,
                entities: Dict[str, List[Any]],
                feature_service: Optional[str] = None,
                features: Optional[List[str]] = None,
                full_feature_names: bool = False,
                query_embedding: Optional[List[float]] = None,
                query_string: Optional[str] = None,
        ) -> Dict[str, Any]:
            """
            Retrieves online documents from the Feast feature server.

            Args:
                entities (Dict[str, List[Any]]): A dictionary of entity names to lists of entity values.
                feature_service (Optional[str]): The name of the feature service to use.
                features (Optional[List[str]]): A list of feature names to retrieve.
                full_feature_names (bool): Whether to return full feature names.
                query_embedding (Optional[List[float]]): Query embedding for document retrieval.
                query_string (Optional[str]): Query string for document retrieval.

            Returns:
                Dict[str, Any]: A dictionary containing the online documents.
            """
            url = f"{self.feature_server_url}/retrieve-online-documents"
            headers = {"Content-Type": "application/json", "accept": "application/json"}
            payload = {
                "entities": entities,
                "feature_service": feature_service,
                "features": features,
                "full_feature_names": full_feature_names,
                "query_embedding": query_embedding,
                "query_string": query_string,
            }
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()

        def push(
                self,
                push_source_name: str,
                df: Dict[str, List[Any]],
                to: str = "online",
                allow_registry_cache: bool = True,
                transform_on_write: bool = True,
        ) -> None:
            """
            Pushes features to the Feast feature server.

            Args:
                push_source_name (str): The name of the push source.
                df (Dict[str, List[Any]]): A dictionary representing the DataFrame to push.
                to (str): The destination to push the features to ("online", "offline", or "online_and_offline").
                allow_registry_cache (bool): Whether to allow the registry cache.
                transform_on_write (bool): Whether to transform the features on write.
            """
            url = f"{self.feature_server_url}/push"
            headers = {"Content-Type": "application/json", "accept": "application/json"}
            payload = {
                "push_source_name": push_source_name,
                "df": df,
                "to": to,
                "allow_registry_cache": allow_registry_cache,
                "transform_on_write": transform_on_write,
            }
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

        def write_to_online_store(
                self,
                feature_view_name: str,
                df: Dict[str, List[Any]],
                allow_registry_cache: bool = True,
                transform_on_write: bool = True,
        ) -> None:
            """
            Writes data to the online store using the Feast feature server.

            Args:
                feature_view_name (str): The name of the feature view.
                df (Dict[str, List[Any]]): A dictionary representing the DataFrame to write.
                allow_registry_cache (bool): Whether to allow the registry cache.
                transform_on_write (bool): Whether to transform the features on write.
            """
            url = f"{self.feature_server_url}/write-to-online-store"
            headers = {"Content-Type": "application/json", "accept": "application/json"}
            payload = {
                "feature_view_name": feature_view_name,
                "df": df,
                "allow_registry_cache": allow_registry_cache,
                "transform_on_write": transform_on_write,
            }
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

        def health(self) -> bool:
            """
            Checks the health of the Feast feature server.

            Returns:
                bool: True if the server is healthy, False otherwise.
            """
            try:
                url = f"{self.feature_server_url}/health"
                response = requests.get(url)
                return response.status_code == 200
            except requests.exceptions.RequestException:
                return False

        def materialize(
                self, start_ts: str, end_ts: str, feature_views: Optional[List[str]] = None
        ) -> None:
            """
            Materializes features for a given time range.

            Args:
                start_ts (str): The start timestamp for materialization.
                end_ts (str): The end timestamp for materialization.
                feature_views (Optional[List[str]]): A list of feature view names to materialize.
            """
            url = f"{self.feature_server_url}/materialize"
            headers = {"Content-Type": "application/json", "accept": "application/json"}
            payload = {"start_ts": start_ts, "end_ts": end_ts, "feature_views": feature_views}
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

        def materialize_incremental(self, end_ts: str, feature_views: Optional[List[str]] = None) -> None:
            """
            Materializes features incrementally up to a given timestamp.

            Args:
                end_ts (str): The end timestamp for incremental materialization.
                feature_views (Optional[List[str]]): A list of feature view names to materialize.
            """
            url = f"{self.feature_server_url}/materialize-incremental"
            headers = {"Content-Type": "application/json", "accept": "application/json"}
            payload = {"end_ts": end_ts, "feature_views": feature_views}
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

    # Read the processed data
    df = pd.read_parquet(input_path + "/processed_data.parquet")

    # Initialize FeatureStore and write to online store
    print("directory = ", os.listdir("./"))

    feast_client = FeastClient(feature_server_url="http://localhost:8081")
    feast_client.write_to_online_store(feature_view_name="docling_feature_view", df=data_dict)


@dsl.pipeline(name="Docling PDF Processing Pipeline")
def convert_pipeline():
    # Step 1: Import PDFs
    importer = import_test_pdfs()

    # Step 2: Process PDFs (convert, chunk, and embed)
    processed_data = process_pdfs(input_path=importer.output)

    # Step 3: Store the processed data into Feast
    store_in_feast(input_path=processed_data.output)

if __name__ == "__main__":
    import kfp
    output_yaml = "docling_pipeline.yaml"
    kfp.compiler.Compiler().compile(convert_pipeline, output_yaml)
    print(f"\nDocling pipeline compiled to {output_yaml}")
    print("running pipeline")
    convert_pipeline()

