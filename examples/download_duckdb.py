import os
from inspect import getsourcefile
from os.path import dirname

from pipebio.models.export_format import ExportFormat
from pipebio.pipebio_client import PipebioClient

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

# Export the full document as a DuckDB database (.db) in a ZIP archive.
# Open the .db with duckdb.connect(path) or ATTACH, not read_parquet.
destination_dir = dirname(getsourcefile(lambda: 0))
client.export(document_id, ExportFormat.DUCKDB, destination_dir)
print(f'DuckDB export saved to {destination_dir}')
