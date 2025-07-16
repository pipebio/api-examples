import os
from pipebio.pipebio_client import PipebioClient


client = PipebioClient(url='https://app.pipebio.com')
shareable_id = os.environ['TARGET_SHAREABLE_ID']

entities = client.shareables.list_entities(shareable_id)

# Include trailing dot to exclude the target folder.
children = list(e for e in entities if e['path'].startswith('1698616.1700043.'))