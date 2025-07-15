import csv
import os
from inspect import getsourcefile
from os.path import dirname
from pipebio.column import Column
from pipebio.models.entity_types import EntityTypes
from pipebio.models.sequence_document_kind import SequenceDocumentKind
from pipebio.models.table_column_type import TableColumnType
from pipebio.models.upload_summary import UploadSummary
from pipebio.pipebio_client import PipebioClient
from pipebio.uploader import Uploader

folder_id = int(os.environ['TARGET_FOLDER_ID'])
shareable_id = os.environ['TARGET_SHAREABLE_ID']
sequence_document_kind = SequenceDocumentKind.DNA

client = PipebioClient(url='https://app.pipebio.com')

# Get file to upload.
file_name = 'upload.tsv'
current_dir = dirname(getsourcefile(lambda: 0))
file_path = os.path.join(current_dir, f'../sample_data/{file_name}')

# First create the entity
new_entity = client.entities.create_file(
    project_id=shareable_id,
    name=file_name,
    parent_id=folder_id,
    entity_type=EntityTypes.SEQUENCE_DOCUMENT,
)
new_entity_id = new_entity['id']
print(f"Created new_entity {new_entity_id} {file_name}")

# Specify the column schema
input_columns = [
    # id, name, description, labels added automatically
    Column(header='sequence', type=TableColumnType.STRING),
]

uploader = Uploader(new_entity_id, input_columns, client.sequences)

# Write the rows from the TSV file
write_count = 0
with open(file_path, 'r') as file:
    for row in csv.DictReader(file, delimiter='\t'):
        uploader.write_data(row)
        write_count = write_count + 1

# Upload the file contents
uploader.upload()

summary = UploadSummary(
    new_entity_id,
    sequence_count=write_count,
    sequence_document_kind=sequence_document_kind
)

# Finally, mark the file as visible
client.entities.mark_file_visible(summary)
