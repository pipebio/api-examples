from pipebio.models.job_type import JobType
from pipebio.pipebio_client import PipebioClient

client = PipebioClient(url='https://app.pipebio.com')

# Set your AWS bucket here.
bucket_name = 'TODO'
# Provide a destination project id (aka shareableId).
# You can browse projects here: https://app.pipebio.com
# And copy their ids by turning table view on (button on right side of screen) and
# then in the table toggling the 3 dots and choosing columns.
# More details here: @see https://app.pipebio.com/api/documentation?readmeUrl=docs/project-and-folder-tree
shareable_id = 'TODO'
# Search with a prefix/
prefix = ''
# Filter results by their extensions.
extensions = ','.join(['.fq', '.fastq', '.fq.gz', '.fastq.gz'])
response = client.session.get(f'aws-integration/buckets/{bucket_name}/objects?limit=1000&prefix={prefix}&extensions={extensions}')

items = response.json()
names = list(item['key'] for item in items['data'])

if len(names) == 0:
    raise Exception(f'No objects found matching prefix "{prefix}" and extensions "{extensions}"')

job = client.jobs.create(
    shareable_id=shareable_id,
    input_entity_ids=[],
    job_type=JobType.AwsImportJob,
    name='AWS Import from SDK',
    params={
        "bucketFiles": [
            {
                "files": names,
                "bucket": bucket_name
            }
        ],
        # If you like to import the PipeBio data into a specific folder within the project.
        "targetFolderId": None
    },
    poll_jobs=True
)

# If you want to start a workflow now you could do that here, using job['outputEntities'] as inputs to another job.
print(job)