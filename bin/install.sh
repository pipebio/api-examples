# Install uv if not already installed
pip install uv

# Use --system flag for CI environments where we don't need a virtual environment
uv pip install --system -r requirements.txt