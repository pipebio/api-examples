"""
AWS S3 import example.

This example uses AwsImportJob, the documented public API for importing files from S3.
Provide object keys explicitly in bucketFiles below.

Note: PipeBio also exposes an internal aws-integration endpoint for listing bucket
objects (GET /api/v2/aws-integration/buckets/{bucketName}/objects). That route is not
part of the public OpenAPI contract and may change without notice. Prefer configuring
AWS import in the PipeBio UI and supplying known object keys here.
"""
import os

from pipebio.models.job_type import JobType
from pipebio.pipebio_client import PipebioClient

client = PipebioClient(url='https://app.pipebio.com')

bucket_name = os.environ.get('AWS_IMPORT_BUCKET_NAME', 'TODO')
shareable_id = os.environ.get('TARGET_SHAREABLE_ID', 'TODO')
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

job = client.jobs.create(
    shareable_id=shareable_id,
    input_entity_ids=[],
    job_type=JobType.AwsImportJob,
    name='AWS Import from SDK',
    params=params,
    poll_jobs=True,
)

print(job)
