import os
from inspect import getsourcefile
from os.path import dirname

from pipebio.pipebio_client import PipebioClient

document_id = os.environ['TARGET_DOCUMENT_ID']

client = PipebioClient(url='https://app.pipebio.com')

current_dir = dirname(getsourcefile(lambda: 0))
assay_file_path = os.path.join(current_dir, '../sample_data/assay_binding_scores.tsv')

# Merge assay columns into the target document by matching assay clone_id to document name.
# This permanently modifies TARGET_DOCUMENT_ID; use a throwaway test document.
# See https://docs.pipebio.com/docs/assay-and-functional-data
job = client.entities.merge(
    entity_id=document_id,
    assay_absolute_file_path=assay_file_path,
    assay_column='clone_id',
    entity_column='name',
    append_unmatched_rows=False,
)

print(f'Merge complete. Job status: {job["status"]}')
