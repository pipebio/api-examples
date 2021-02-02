import os
from library.util import Util
from library.pipebio_client import PipebioClient

#
#  Upload Example
#  Upload a fasta file from the sample_data directory to the project and folder you specify.
#  Please set the `folder_id` and `project_id` below to start.
#

# TODO Set your project id and folder id here.
# Right click a folder in the UI choose "copy id" then paste that here.
folder_id = None
# Open a project in the app, click "edit" and then copy the project id from there.
project_id = ''

if folder_id is None or project_id is None:
    raise Exception("Error! Set folder_id and project_id to continue.")

client = PipebioClient()

# Either login with hardcoded variables or use environment variables.
# PIPE_EMAIL=<my-email> PIPE_PASSWORD=<my-password> PIPE_TOKEN=<my-token> python login.py
client.login()

# Display who we are logged in as.
user = client.authentication.user
print('\nLogged in as {}. \n'.format(user['firstName'], user['lastName']))

# Upload a sample file to the
file_name = '137_adimab_VL.fsa'
file_path = os.path.join(Util.get_executed_file_location(), '../sample_data/adimab/{}'.format(file_name))
client.upload_file(
    file_name=file_name,
    absolute_file_location=file_path,
    parent_id=folder_id,
    project_id=project_id,
    organization_id=user['orgs'][0]['id']
)

print('\nDone.\n\n')
