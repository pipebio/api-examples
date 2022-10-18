import csv
import os

from library.column import Column
from library.models import EntityTypes
from library.models.sequence_document_kind import SequenceDocumentKind
from library.models.table_column_type import TableColumnType
from library.models.upload_summary import UploadSummary
from library.pipebio_client import PipebioClient
from library.uploader import Uploader
from library.util import Util


def example_01a_upload_example_fasta():
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

    # Display api key user details.
    user = client.user
    print('\nUsing api key for {}. \n'.format(user['firstName'], user['lastName']))

    # Get a list of all available projects for the user's organization.
    projects = client.shareables.list()

    # Find a specific project having a name "Example".
    example_project = next((project for project in projects if project['name'] == project_name), None)
    if example_project is None:
        raise Exception(f'Error: Example project named "{project_name}" not found')

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


def example_01b_upload_example_tsv():
    project_name = os.environ['PROJECT_NAME']
    folder_id = os.environ['TARGET_FOLDER_ID']
    sequence_document_kind = SequenceDocumentKind.DNA

    if folder_id is None or project_name is None:
        raise Exception("Error! Set folder_id and project_name to continue.")
    else:
        folder_id = int(folder_id)

    client = PipebioClient()

    # Display api key user details.
    user = client.user
    print('\nUsing api key for {}. \n'.format(user['firstName'], user['lastName']))

    # Get a list of all available projects for the user's organization.
    projects = client.shareables.list()

    # Find a specific project having a name "Example".
    example_project = next((project for project in projects if project['name'] == project_name), None)
    if example_project is None:
        raise Exception(f'Error: Example project named "{project_name}" not found')

    # Upload a sample file to the
    file_name = 'upload.tsv'
    file_path = os.path.join(Util.get_executed_file_location(), '../sample_data/{}'.format(file_name))

    # First create the entity
    new_entity = client.entities.create_file(
        project_id=example_project['id'],
        name=file_name,
        parent_id=folder_id,
        entity_type=EntityTypes.SEQUENCE_DOCUMENT,
    )
    new_entity_id = new_entity['id']
    print("created new_entity {} {}".format(new_entity_id, file_name))

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

    return summary


if __name__ == "__main__":
    example_01a_upload_example_fasta()
