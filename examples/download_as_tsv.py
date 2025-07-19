import os
from pipebio.pipebio_client import PipebioClient
from pipebio.util import Util

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

absolute_location = os.path.join(Util.get_executed_file_location(), 'MyDocument.tsv')
client.sequences.download(document_id, destination=absolute_location)
