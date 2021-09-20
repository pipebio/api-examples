import os

from library.models.export_format import ExportFormat
from library.pipebio_client import PipebioClient
from library.util import Util


def login_and_get_target_folder():
    client = PipebioClient()

    # Either login with hardcoded variables or use environment variables.
    # PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
    client.login()

    # Display who we are logged in as.
    user = client.authentication.user
    print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

    # Get a reference to the folder this file is in.
    return client


def get_sequence_id():
    sequence_document_id = os.environ['TARGET_DOCUMENT_ID']

    if sequence_document_id is None:
        raise Exception("Error! Set sequence_document_id to continue.")
    else:
        return int(sequence_document_id)


def example_02a_download_result_as_tsv(document_id:int):
    """
    Download the raw file as a TSV.
    """
    client = login_and_get_target_folder()

    # Specify a target folder on this computer to download the file to.
    this_folder = Util.get_executed_file_location()

    return client.sequences.download(document_id, destination=this_folder)


def example_02b_download_result_to_memory_to_do_more_work(document_id: int):
    """
    Download the file into memory to do more work
    """
    client = login_and_get_target_folder()

    return client.sequences.download_to_memory([document_id])


def example_02c_download_result_to_biological_format(document_id):
    """
    Download the format in Genbank, Fasta, Fastq, Ab1 etc.
    """
    client = login_and_get_target_folder()

    # Specify a target folder on this computer to download the file to.
    this_folder = Util.get_executed_file_location()

    return client.export(document_id, ExportFormat.GENBANK.value, this_folder)


def example_02d_download_original_file(document_id: int, destination_filename: str):
    """
    Download the original, un-parsed file.
   """
    client = login_and_get_target_folder()

    return client.entities.download_original_file(document_id, destination_filename)
