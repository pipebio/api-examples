import os
from library.util import Util
from library.pipebio_client import PipebioClient


def example_01_upload_example():
    """
    Upload Example
    Upload a fasta file from the sample_data directory to the project and folder you specify.
    Please set the `folder_id` and `project_id` below to start.
    :return:
    """

    project_name = os.environ['PROJECT_NAME']
    folder_id = os.environ['TARGET_FOLDER_ID']

    if folder_id is None or project_name is None:
        raise Exception("Error! Set folder_id and project_name to continue.")
    else:
        folder_id = int(folder_id)

    client = PipebioClient()

    # Either login with hardcoded variables or use environment variables.
    # PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
    client.login()

    # Display who we are logged in as.
    user = client.authentication.user
    print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

    # Get a list of all available projects for the user's organization.
    projects = client.shareables.list()

    # Find a specific project having a name "Example".
    example_project = next((project for project in projects if project['name'] == project_name), None)
    if example_project is None:
        print(f'Error: Example project named "{project_name}" not found')
        quit()

    # Upload a sample file to the
    file_name = '137_adimab_VL.fsa'
    file_path = os.path.join(Util.get_executed_file_location(), '../sample_data/adimab/{}'.format(file_name))

    return client.upload_file(
        file_name=file_name,
        absolute_file_location=file_path,
        parent_id=folder_id,
        project_id=example_project['id'],
        organization_id=user['orgs'][0]['id']
    )


if __name__ == "__main__":
    example_01_upload_example()