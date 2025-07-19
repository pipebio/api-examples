import os
from pipebio.pipebio_client import PipebioClient

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

client.sequences.download_to_memory([document_id])
