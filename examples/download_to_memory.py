import os
from pipebio.pipebio_client import PipebioClient

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

# iter_sequence_records exports the document and streams it back one record at
# a time, so memory use stays flat however large the document is. Each item is
# a (compound_id, record) pair, where compound_id is
# "<document_id>##@##<sequence_id>". The server does not guarantee an ordering,
# so treat the records as an unordered stream.
record_count = 0
for compound_id, record in client.iter_sequence_records([document_id]):
    if record_count == 0:
        sequence_name = record['name']
        print(f'Sample record {compound_id} is named {sequence_name}')
    record_count += 1

print(f'Streamed {record_count} records without holding them all in memory')

# Build the complete map only when the whole document must be available at
# once. This reproduces what the deprecated
# client.sequences.download_to_memory() returned, including its memory cost:
#   records = dict(client.iter_sequence_records([document_id]))
