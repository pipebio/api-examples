from library.models.export_format import ExportFormat
from library.pipebio_client import PipebioClient
from library.util import Util
#
#  Download Example
#
#  Please set the `sequence_document_id` below to start.
#
#  3 examples of how you can download:
#   a) the raw file as a tsv
#   b) the file into memory to do more work
#   c) the file in a format you like.
#

client = PipebioClient()

# TODO Set sequence_document_id here.
# Right click a folder in the UI choose "copy id" then paste that here.
sequence_document_id = None

if sequence_document_id is None:
    raise Exception("Error! Set sequence_document_id to continue.")

# Either login with hardcoded variables or use environment variables.
# PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
client.login()

# Display who we are logged in as.
user = client.authentication.user
print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

# Get a reference to the folder this file is in.
this_folder = Util.get_executed_file_location()

# Example 1.
#   - Download a file by id to a destination you specify.
#   - Specify a destination (e.g. ~/Desktop/my-file.tsv) to use that destination instead.
client.sequences.download(sequence_document_id, destination=this_folder)

# Example 2.
#   - Download a sequence-file by id into memory.
rows = client.sequences.download_to_memory([sequence_document_id])

# Example 3.
#   - Download a sequence-file in a particular format.
client.export(sequence_document_id, ExportFormat.GENBANK.value, this_folder)

print('\nDone.\n\n')
