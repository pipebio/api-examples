[![CI](https://github.com/pipebio/api-examples/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/pipebio/api-examples/actions/workflows/main.yml)

# API Examples

- Examples showing how to interact with the Pipe|bio REST API through Python.
- See [https://docs.pipebio.com](https://docs.pipebio.com) for full API documentation.
- See [PyPI](https://pypi.org/project/pipebio/) for our SDK, which wraps some methods in the API.
- Examples currently wrap our API endpoints in Python only. You can, of course, use the endpoints in any language (Java, JavaScript, C#, etc.).
- All endpoints are under heavy development and subject to change. Use at your own risk.

## Installation

- [Python3](https://wsvincent.com/install-python3-mac/) or just `brew install python@3.10`
- Install uv: `pip install uv` or follow [uv installation guide](https://github.com/astral-sh/uv)
- Create a virtual environment and install dependencies in one step: `uv venv && source venv/bin/activate && uv pip install -r requirements.txt`
- Alternatively, step-by-step:
  - Create a virtual environment: `uv venv`
  - Activate the venv: `source venv/bin/activate`
  - Install dependencies: `uv pip install -r requirements.txt`

## Getting started

- Check out the files in the `examples` directory; to run them, use `python examples/upload_fasta.py`, for example.
- For local Parquet and DuckDB file conversion, see [the data conversion examples](examples/data_conversion.md).
- Before running the examples, make sure to set the required environment variables:
  ```bash
  export TARGET_FOLDER_ID="your_folder_id"
  export TARGET_SHAREABLE_ID="your_shareable_id"
  export TARGET_DOCUMENT_ID="your_document_id"
  export AWS_IMPORT_BUCKET_NAME="your_bucket_name"  # for import_from_aws.py only
  ```
- Please contact support@pipebio.com for help.

### Example overview

| Script | What it demonstrates |
|--------|---------------------|
| `upload_fasta.py` | Upload a FASTA file via signed URL |
| `upload_tsv.py` | Create a document and upload TSV rows |
| `download_as_tsv.py` | Export a document as TSV via `client.export()` |
| `download_as_duckdb.py` | Export a document as DuckDB via `client.export()` |
| `download_original_file.py` | Download the original uploaded file for a document |
| `download_as_parquet.py` | Export a document as Parquet via `client.export()` |
| `download_to_genbank.py` | Export a document as GenBank via `client.export()` |
| `download_to_memory.py` | Stream sequence records via `client.iter_sequence_records()` |
| `entities_check_if_folder_exists.py` | Check whether a folder name exists in a project |
| `entities_get_children_of_folder.py` | List child entities under a folder path |
| `import_from_aws.py` | Import known S3 object keys via `AwsImportJob` |
| `merge_assay_data.py` | Merge assay data into a document via `client.entities.merge()` |
| `run_extract_job.py` | Run an Extract job |
| `workflows_example.py` | Upload files and run a workflow |

## Running Tests

To ensure these examples work correctly, we have integration tests that run each example:

```bash
# Install pytest
uv pip install pytest

# Run the integration tests
python -m pytest -c pytest.ini -v
```

The tests will check that each example script can run to completion without errors.

## Contributing

- Issues and pull requests are welcome.
