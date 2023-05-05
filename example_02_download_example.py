import os

from pipebio.models.export_format import ExportFormat
from pipebio.pipebio_client import PipebioClient
from pipebio.util import Util


def get_sequence_id():
    sequence_document_id = os.environ['TARGET_DOCUMENT_ID']

    if sequence_document_id is None:
        raise Exception("Error! Set sequence_document_id to continue.")
    else:
        return int(sequence_document_id)


def example_02a_download_result_as_tsv(document_id: int, destination_filename: str = None, destination_location: str = None):
    """
    Download the raw file as a TSV.
    """
    client = PipebioClient()

    # Display api key user details.
    user = client.user
    print('\nUsing api key for {} {}. \n'.format(user['firstName'], user['lastName']))

    # Set the download name and folder.
    destination_filename = "download.tsv" if destination_filename is None else destination_filename
    destination_location = Util.get_executed_file_location() if destination_location is None else destination_location
    absolute_location = os.path.join(destination_location, destination_filename)

    client.sequences.download(document_id, destination=absolute_location)

    return absolute_location


def example_02b_download_result_to_memory_to_do_more_work(document_id: int):
    """
    Download the file into memory to do more work
    """
    client = PipebioClient()

    # Display api key user details.
    user = client.user
    print('\nUsing api key for {} {}. \n'.format(user['firstName'], user['lastName']))

    return client.sequences.download_to_memory([document_id])


def example_02c_download_result_to_biological_format(document_id, destination_location: str = None):
    """
    Download the format in Genbank, Fasta, Fastq, Ab1 etc.
    """
    client = PipebioClient()

    # Display api key user details.
    user = client.user
    print('\nUsing api key for {} {}. \n'.format(user['firstName'], user['lastName']))

    # Specify a target folder on this computer to download the file to.
    destination_location = Util.get_executed_file_location() if destination_location is None else destination_location

    return client.export(document_id, ExportFormat.GENBANK.value, destination_location)


def example_02d_download_original_file(document_id: int, destination_filename: str = None, destination_location: str = None) -> str:
    """
    Download the original, un-parsed file.
   """
    client = PipebioClient()

    # Display api key user details.
    user = client.user
    print('\nUsing api key for {} {}. \n'.format(user['firstName'], user['lastName']))

    # Set the download name and folder.
    destination_filename = "download.tsv" if destination_filename is None else destination_filename
    # Specify a target folder on this computer to download the file to.
    destination_location = Util.get_executed_file_location() if destination_location is None else destination_location
    absolute_location = os.path.join(destination_location, destination_filename)

    return client.entities.download_original_file(document_id, absolute_location)


if __name__ == "__main__":
    # Downloads the document as a tsv and prints the file location
    example_02a_download_result_as_tsv(1063911, '1063911_download', '/Users/Chris/temp')

    # Prints document content
    print(example_02b_download_result_to_memory_to_do_more_work(1063911))

    # Downloads the file in biological format and prints the file location
    print(example_02c_download_result_to_biological_format(1063911, '/Users/Chris/temp'))

    # Downloads the original file and prints the file location
    print(example_02d_download_original_file(1063895, '1063911_original', '/Users/Chris/temp'))

