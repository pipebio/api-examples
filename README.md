[![CI](https://github.com/pipebio/api-examples/actions/workflows/main.yml/badge.svg)](https://github.com/pipebio/api-examples/actions/workflows/main.yml)

# API Examples

- Examples showing how to interact with the Pipe|bio REST api through python.
- See [https://docs.pipebio.com](https://docs.pipebio.com) for full API documentation.
- See [PyPi](https://pypi.org/project/pipebio/) for our SDK which wraps some methods in the API
- Examples currently wrap our API endpoints in python only, you can of course use the endpoints in any language (java, javascript, c#, etc)
- All endpoints are under heavy development and subject to change. Use at your own risk.

## Installation

- [Python3](https://wsvincent.com/install-python3-mac/) or just `brew install python@3.7`
- Install uv: `pip install uv` or follow [uv installation guide](https://github.com/astral-sh/uv)
- Create a virtual environment and install dependencies in one step: `uv venv && source venv/bin/activate && uv pip install -r requirements.txt`
- Alternatively, step by step:
  - Create a virtual environment: `uv venv`
  - Activate the venv: `source venv/bin/activate`
  - Install dependencies: `uv pip install -r requirements.txt`

## Getting started

- Check out the files with names in the `examples` directory; to run them use `python examples/upload_fasta.py` for example.
- Before running the examples, make sure to set the required environment variables:
  ```bash
  export TARGET_FOLDER_ID="your_folder_id"
  export TARGET_SHAREABLE_ID="your_shareable_id"
  export TARGET_DOCUMENT_ID="your_document_id"
  ```
- Please contact support@pipebio.com for help.

## Running Tests

To ensure these examples work correctly, we have integration tests that run each example:

```bash
# Install pytest
uv pip install pytest

# Run the integration tests
python -m pytest -c pytest.ini
```

The tests will check that each example script can run to completion without errors.

## Contributing

- Issues and pull requests are welcome.
