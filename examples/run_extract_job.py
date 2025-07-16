import os
from pipebio.pipebio_client import PipebioClient
from pipebio.models.job_type import JobType

client = PipebioClient(url='https://app.pipebio.com')
shareable_id = os.environ['TARGET_SHAREABLE_ID']
folder_id = int(os.environ['TARGET_FOLDER_ID'])
document_id = int(os.environ['TARGET_DOCUMENT_ID'])



# Create a new folder to put results into.
new_folder = client.entities.create_folder(
    project_id=shareable_id,
    name='New folder',
    parent_id=folder_id,
    visible=True
)

# Run an extract job and put the results in the new_folder we just created.
job_id = client.jobs.create(
    shareable_id=shareable_id,
    name='Extract job from API',
    job_type=JobType.ExtractJob,
    # Either hard code input entity ids or get these from the output_entity_ids from a previous job.
    input_entity_ids=[document_id],
    params={
        "regions": None,
        "translate": False,
        "targetFolderId": new_folder['id'],
        "alignmentOutput": False,
        "translationTable": "Standard",
        "ignoreEmptySequences": True
    },
    poll_jobs=True
)

# Fetch the job.
job = client.jobs.get(job_id)

# If you need to do something more, you can get the output entities like this:
output_id = next(e['id'] for e in job['outputEntities'])

# For example, if you wanted to cluster the results:
#
# job_id = client.jobs.create(
#     shareable_id=shareable_id,
#     name='Extract job from API',
#     job_type=JobType.ClusterJob,
#     # Either hard code input entity ids or get these from the output_entity_ids from a previous job.
#     input_entity_ids=[output_id],
#     params={
#         "regions": None,
#         "translate": False,
#         "targetFolderId": 1700043,
#         "alignmentOutput": False,
#         "translationTable": "Standard",
#         "ignoreEmptySequences": True
#     },
#     poll_jobs=True
# )
