import os
from pipebio.pipebio_client import PipebioClient
from pipebio.models.job_type import JobType

client = PipebioClient(url='https://app.pipebio.com')
shareable_id = os.environ['TARGET_SHAREABLE_ID']
folder_id = os.environ['TARGET_FOLDER_ID']

document_id = 'ent_PbyAluxijmJfNXph'

# Summarise 1 or more documents using a summary job.
job_id = client.jobs.create(
    shareable_id=shareable_id,
    name='Summary job from SDK',
    job_type=JobType.SummaryJob,
    # Include 1 or more document ids here to summarize in one go.
    input_entity_ids=[document_id],
    params={
        "targetFolderId": folder_id
    },
    poll_jobs=True
)

# Fetch the job.
job = client.jobs.get(job_id)

# Get the id of the report that was created.
output_id = next(e['id'] for e in job['outputEntities'])

# Fetch the summary report we just created.
response = client.session.get(f'entities/{output_id}/attachments/Report')
result = response.json()
annotation_summary = next(item for item in result['data']['items'] if item['title'].startswith('Analysis results for'))

# Print the results.
# e.g:
#   Id: SRR11974622
#   Total reads: 1059733
#   Correct annotated: 320376
#   Correct annotated percent: 30.2317659259455
for row in annotation_summary['data']['rows']:
    for col in annotation_summary['data']['columns']:
        header = col['headerName']
        field = col['field']
        value = row[field]
        print(f'{header}: {value}')
