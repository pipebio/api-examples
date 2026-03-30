from pathlib import Path
from pipebio.pipebio_client import PipebioClient
from pipebio.uploader import Uploader


def run_workflow():
    '''
    This is not supported for users yet :( 
    I have contacted Pipebio
    '''
    #TODO: Implement this for our workflow(s)
    raise NotImplementedError(f'Currently there are no workflows for Protillion')
    # Create a folder to store the results of this workflow.
    # workflow_folder = client.entities.create_folder(
    #     project_id=current_project_id,
    #     name='Trial WF',
    #     parent_id=target_folder_id,
    #     # Optionally hide the workflow results until the Workflow is done.
    #     # While you're working on the workflow you may like to hide hide the folder and set it visible afterwards.
    #     visible=True,
    # )
    # workflow_folder_id = workflow_folder['id']
    # workflow_folder_name = workflow_folder['name']
    # workflow_folder_path = workflow_folder['path']
    # print(
    #     f'Created folder "{workflow_folder_name}", '
    #     f'here: {base_url}/api/v2/entities/_open?entityIds={workflow_folder_id}'
    # )

    # Run the workflow.
    # workflow_job = client.workflows.run_workflow(
    #     project_id=current_project_id,
    #     # TODO Replace with your own workflow id.
    #     workflow_id='d36548eb-ad4a-4c66-a127-4c2027eec7a4',
    #     name='Annotate, pair and cluster',
    #     input_entity_ids=uploaded_ids,
    #     target_folder_id=workflow_folder['id'],
    #     params={
    #         'germlineIds': [germline_id],
    #         'scaffold': 'IgG'
    #     },
    #     poll_job=False,
    # )

    # result = client.jobs.poll_job(
    #     job_id=workflow_job['id'],
    #     # Set a long timeout (5 hours).
    #     timeout_seconds=5 * 60 * 60
    # )

def upload_files(file_dir, file_ext='.*\.csv'):
    upload_jobs = client.upload_files(
        absolute_folder_path=file_dir,
        parent_id=target_folder_id,
        project_id=current_project_id,
        filename_pattern=file_ext,
        poll_jobs=True
    )

def upload_file():
    client.upload_file(
        file_name=file_name,
        absolute_file_location=file_path,
        parent_id=folder_id,
        project_id=shareable_id,
    )


def get_entity_info(project, run_group_name, run_name, folder_name):
    project_info = client.shareables.get_project(project)
    project_id = project_info['id']
    all_entities = client.shareables.list_entities(project_id)
    top_level_entities = []
    entity_group_name, entity_run_name, entity_folder_name = ''
    for entity in all_entities:
        if run_group_name and (entity['name'] == run_group_name):
            entity_group_name = entity
        elif entity['name'] == run_name:
            entity_run_name = entity
        elif entity['name'] == folder_name:
            entity_folder_name = folder_name
        if '.' not in entity['path']:
            top_level_entities.append(entity)
    if run_group_name and not entity_group_name:
        #create run group
        run_group_folder = client.entities.create_folder(
            project_id=project_id,
            name=run_group_name,
            visible=True
        )
    else:
        run_group_folder = {}
    
    if entity_run_name:
        logging.warning(f'A run by the same nam is already registered in PipeBio {entity_run_name}')
    
    if run_group_folder:
        #create 
        run_folder_entity = client.entities.create_folder(
            project_id=project_id,
            name=run_name,
            parent_id=run_group_folder['id'],
            visible=True,
        )
    else:
        run_folder_entity = client.entities.create_folder(
            project_id=project_id,
            name=run_name,
            visible=True,
        )
    
    data_folder = client.entities.create_folder(
            project_id=project_id,
            name=folder_name,
            parent_id=run_folder_entity['id'],
            visible=True,
        )
    return data_folder


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--project",
    type=str,
    help="project name (e.g. hermes)",
)
@click.option(
    "--file_path",
    type=str,
    help="path to file you wish to upload",
)
@click.option(
    "--run_group_name",
    type=str,
    default="",
    help="Name of run_group. If not provided, will be skipped.",
)
@click.option(
    "--run_name",
    type=str,
    default="",
    help="Name of run. If not provided, will parse from file_path.",
)
@click.option(
    "--folder_name",
    type=str,
    default="",
    help="Name of folder. If not provided, will parse from file_path.",
)
@click.option(
    "--base_url",
    default="https://protillion.pipebio.benchling.com",
    show_default=True,
    help="url for the organization. Probably don't need to change this.",
)
def main(project, file_path, run_group_name, run_name, folder_name, base_url):

    #parse file and folder names
    file_path = Path(file_path)
    file_name = file_path.parts[-1]
    if not folder_name:
        folder_name = file_path.parts[-2]
    #fitting = file_path.parts[-3]
    if not run_name:
        run_name = file_path.parts[-4]

    #set up client
    client = PipebioClient(url=base_url)
    #get folder info
    data_folder = get_entity_info(project, run_group_name, run_name, folder_name)

    #upload 
    client.upload_file(
        file_name=file_name,
        absolute_file_location=str(file_path),
        parent_id=data_folder['id'],
        project_id=data_folder['ownerId'],
    )

if __name__ == '__main__':
    main()
