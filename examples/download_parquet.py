import os
from inspect import getsourcefile
from os.path import dirname

from pipebio.models.export_format import ExportFormat
from pipebio.pipebio_client import PipebioClient

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

# Export the full document as Parquet files packaged in a ZIP archive.
destination_dir = dirname(getsourcefile(lambda: 0))
client.export(document_id, ExportFormat.PARQUET, destination_dir)
print(f'Parquet export saved to {destination_dir}')
