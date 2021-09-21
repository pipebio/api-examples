[![CI](https://github.com/pipebio/api-examples/actions/workflows/main.yml/badge.svg)](https://github.com/pipebio/api-examples/actions/workflows/main.yml)

# API Examples

- Examples showing how to interact with the Pipe|bio REST api through python.
- See [https://docs.pipebio.com](https://docs.pipebio.com) for full API documentation.
- Examples currently wrap our API endpoints in python only, you can of course use the endpoints in any language (java, javascript, c#, etc)
- All endpoints are under heavy development and subject to change. Use at your own risk.

## Installation

- [Python3](https://wsvincent.com/install-python3-mac/) or just `brew install python@3.7`
- Virtualenv: `pip3 install virtualenv`
- Create a virtual environment: `virtualenv -p python3 venv`
- Activate the venv: `source venv/bin/activate`
- Install dependencies: `pip install -r requirements.txt`

## Getting started

- Check out the files with names like `example*.py`; to run them use `python example_01_upload_example.py` for example.
- To ensure these files are up to date and really work, we have integration tests running in `example_itest.py`.
- Please contact owen@pipebio.com for help.

## Contributing

- Issues and pull requests are welcome.
