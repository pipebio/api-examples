import os
import shutil
from urllib.request import urlopen

from pipebio.pipebio_client import PipebioClient
from pipebio.util import Util

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

# Export the full document as Parquet shards (DUCKDB format) with no SQL filter.
# The Python SDK does not yet wrap _extractV2; use the REST endpoint directly.
response = client.session.post(
    f'entities/{document_id}/_extractV2',
    json={
        'format': 'DUCKDB',
    },
)
Util.raise_detailed_error(response)

signed_urls = response.json()
if not isinstance(signed_urls, list):
    raise ValueError(f'Expected a list of signed URLs, got: {type(signed_urls).__name__}')
if len(signed_urls) == 0:
    raise ValueError(f'No download links returned for document {document_id}')

destination_dir = Util.get_executed_file_location()
downloaded_paths = []

for index, signed_url in enumerate(signed_urls):
    if not isinstance(signed_url, str):
        raise ValueError(f'Expected signed URL strings, got item {index}: {type(signed_url).__name__}')

    destination = os.path.join(destination_dir, f'document-shard-{index}.parquet')
    with urlopen(signed_url) as remote_file:
        with open(destination, 'wb') as local_file:
            shutil.copyfileobj(remote_file, local_file)
    downloaded_paths.append(destination)
    print(f'Downloaded shard {index + 1}/{len(signed_urls)} to {destination}')

# Open the shards with DuckDB (pip install duckdb) or PyArrow.
# Example:
#   import duckdb
#   relation = duckdb.read_parquet(downloaded_paths)
#   print(relation.limit(5).fetchdf())
print('Parquet shards ready for DuckDB or PyArrow.')
