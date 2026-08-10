"""
AWS S3 import example.

Import known S3 object keys via AwsImportJob. Obtain keys from your bucket
inventory or the PipeBio AWS import UI after configuring bucket access.
"""
import os

from pipebio.models.job_type import JobType
from pipebio.pipebio_client import PipebioClient

client = PipebioClient(url='https://app.pipebio.com')

bucket_name = os.environ.get('AWS_IMPORT_BUCKET_NAME') or 'TODO'
shareable_id = os.environ.get('TARGET_SHAREABLE_ID') or 'TODO'
target_folder_id = os.environ.get('TARGET_FOLDER_ID')

# List the S3 object keys to import. Obtain these from your bucket inventory or the
# PipeBio AWS import UI after configuring bucket access.
object_keys = [
    'path/to/sample_R1.fastq.gz',
    'path/to/sample_R2.fastq.gz',
]

if bucket_name == 'TODO' or shareable_id == 'TODO':
    raise ValueError(
        'Set AWS_IMPORT_BUCKET_NAME and TARGET_SHAREABLE_ID before running this example.'
    )

if len(object_keys) == 0:
    raise ValueError('Add at least one S3 object key to object_keys.')

placeholder_prefix = 'path/to/'
if any(key.startswith(placeholder_prefix) for key in object_keys):
    raise ValueError(
        f'Replace placeholder object keys starting with "{placeholder_prefix}" with real S3 keys.'
    )

params = {
    'bucketFiles': [
        {
            'files': object_keys,
            'bucket': bucket_name,
        }
    ],
    'targetFolderId': target_folder_id,
}

job_id = client.jobs.create(
    shareable_id=shareable_id,
    input_entity_ids=[],
    job_type=JobType.AwsImportJob,
    name='AWS Import from SDK',
    params=params,
    poll_jobs=True,
)

print(f'AWS import job {job_id} finished')
