import os
from pipebio.pipebio_client import PipebioClient


client = PipebioClient(url='https://app.pipebio.com')
shareable_id = os.environ['TARGET_SHAREABLE_ID']

entities = client.shareables.list_entities(shareable_id)

search_string = 'Example folder'

exists = any(e for e in entities if e['name'].lower().strip() == search_string.lower().strip())