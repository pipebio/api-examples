from pipebio.pipebio_client import PipebioClient
import os
from inspect import getsourcefile
from os.path import dirname

base_url = 'https://app.pipebio.com'
client = PipebioClient(url=base_url)

# Set the following value to the id of the project you want to run inside.
current_project_id = os.environ['TARGET_SHAREABLE_ID']
# Folder in the demo project
target_folder_id = os.environ['TARGET_FOLDER_ID']
# Set this to a location on your computer where the results should download to.
download_absolute_location = '/tmp'
# Germline id is for Human, IMGT. Make sure to replace with your own.
org_id = client.user['org']['id']
germlines = client.session.get(f'/api/v2/organizations/{org_id}/lists?kind=germline').json()
germline_id = next(germline['id'] for germline in germlines['data'] if germline['name'] == 'Human 🧍 - IMGT (v1.1)')

upload_jobs = client.upload_files(
    absolute_folder_path=os.path.join(dirname(getsourcefile(lambda: 0)), f'../sample_data/adimab/'),
    parent_id=target_folder_id,
    project_id=current_project_id,
    filename_pattern='.*\.fsa',
    poll_jobs=True
)

uploaded_ids = []
for job in upload_jobs:
    for output_entity in job['outputEntities']:
        uploaded_ids.append(output_entity['id'])

# Create a folder to store the results of this workflow.
workflow_folder = client.entities.create_folder(
    project_id=current_project_id,
    name='Trial WF',
    parent_id=target_folder_id,
    # Optionally hide the workflow results until the Workflow is done.
    # While you're working on the workflow you may like to hide hide the folder and set it visible afterwards.
    visible=True,
)
workflow_folder_id = workflow_folder['id']
workflow_folder_name = workflow_folder['name']
workflow_folder_path = workflow_folder['path']
print(
    f'Created folder "{workflow_folder_name}", '
    f'here: {base_url}/api/v2/entities/_open?entityIds={workflow_folder_id}'
)

# Run the workflow.
workflow_job = client.workflows.run_workflow(
    project_id=current_project_id,
    # TODO Replace with your own workflow id.
    workflow_id='d36548eb-ad4a-4c66-a127-4c2027eec7a4',
    name='Annotate, pair and cluster',
    input_entity_ids=uploaded_ids,
    target_folder_id=workflow_folder['id'],
    params={
        'germlineIds': [germline_id],
        'scaffold': 'IgG'
    },
    poll_job=False,
)

result = client.jobs.poll_job(
    job_id=workflow_job['id'],
    # Set a long timeout (5 hours).
    timeout_seconds=5 * 60 * 60
)

all_entities = client.shareables.list_entities(shareable_id=current_project_id)
# Get all annotated sequence documents inside the output folder.
filtered_entities = list(e for e in all_entities if
                         e['path'].startswith(workflow_folder_path)
                         and e['type'] == 'SEQUENCE_DOCUMENT'
                         and e['name'].endswith('- annotated')
                         )

# Download these annotated documents locally to csv.
for output_entity in filtered_entities:
    output_entity_name = output_entity['name']
    client.sequences.download(
        output_entity['id'],
        destination=os.path.join(download_absolute_location, f'{output_entity_name}.tsv')
    )
