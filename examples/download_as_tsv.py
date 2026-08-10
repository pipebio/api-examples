import os
from pipebio.models.export_format import ExportFormat
from pipebio.pipebio_client import PipebioClient
from pipebio.util import Util

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

destination_folder = Util.get_executed_file_location()
destination_filename = 'MyDocument.tsv'

client.export(
    document_id,
    ExportFormat.TSV,
    destination_folder,
    destination_filename=destination_filename,
)
