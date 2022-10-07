import os

from library.models.export_format import ExportFormat
from library.pipebio_client import PipebioClient
from library.util import Util


def get_sequence_id():
    sequence_document_id = os.environ['TARGET_DOCUMENT_ID']

    if sequence_document_id is None:
        raise Exception("Error! Set sequence_document_id to continue.")
    else:
        return int(sequence_document_id)


def example_02a_download_result_as_tsv(document_id: int):
    """
    Download the raw file as a TSV.
    """
    client = PipebioClient()

    # Either login with hardcoded variables or use environment variables:
    # e.g.
    #       client.login(<my-email>, <my-password>, <my-token>)
    #   or:
    #       PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
    client.login()

    # Display who we are logged in as.
    user = client.authentication.user
    print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

    # Set the download name and folder.
    destination_filename = "download.tsv"
    destination_location = Util.get_executed_file_location()
    absolute_location = os.path.join(destination_location, '..', f'Downloads/{destination_filename}')

    client.sequences.download(document_id, destination=absolute_location)

    return absolute_location


def example_02b_download_result_to_memory_to_do_more_work(document_id: int):
    """
    Download the file into memory to do more work
    """
    client = PipebioClient()

    # Either login with hardcoded variables or use environment variables:
    # e.g.
    #       client.login(<my-email>, <my-password>, <my-token>)
    #   or:
    #       PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
    client.login()

    # Display who we are logged in as.
    user = client.authentication.user
    print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

    return client.sequences.download_to_memory([document_id])


def example_02c_download_result_to_biological_format(document_id):
    """
    Download the format in Genbank, Fasta, Fastq, Ab1 etc.
    """
    client = PipebioClient()

    # Either login with hardcoded variables or use environment variables:
    # e.g.
    #       client.login(<my-email>, <my-password>, <my-token>)
    #   or:
    #       PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
    client.login()

    # Display who we are logged in as.
    user = client.authentication.user
    print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

    # Specify a target folder on this computer to download the file to.
    destination_folder = os.path.join(Util.get_executed_file_location(), '..', f'Downloads')

    return client.export(document_id, ExportFormat.GENBANK.value, destination_folder)


def example_02d_download_original_file(document_id: int, destination_filename: str = None) -> str:
    """
    Download the original, un-parsed file.
   """
    client = PipebioClient()

    # Either login with hardcoded variables or use environment variables.
    # PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
    client.login()

    # Display who we are logged in as.
    user = client.authentication.user
    print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

    # Set the download name and folder.
    destination_filename = "download.tsv" if destination_filename is None else destination_filename
    destination_location = Util.get_executed_file_location()
    absolute_location = os.path.join(destination_location, f'../Downloads/{destination_filename}')

    return client.entities.download_original_file(document_id, absolute_location)


if __name__ == "__main__":
    example_02a_download_result_as_tsv(298154)
