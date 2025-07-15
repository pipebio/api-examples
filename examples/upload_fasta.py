import os
from inspect import getsourcefile
from os.path import dirname
from pipebio.pipebio_client import PipebioClient


folder_id = int(os.environ['TARGET_FOLDER_ID'])
shareable_id = os.environ['TARGET_SHAREABLE_ID']

client = PipebioClient(url='https://app.pipebio.com')

# Upload a sample file to the specified project and folder.
file_name = '137_adimab_VL.fsa'
current_dir = dirname(getsourcefile(lambda: 0))
file_path = os.path.join(current_dir, f'../sample_data/adimab/{file_name}')

client.upload_file(
    file_name=file_name,
    absolute_file_location=file_path,
    parent_id=folder_id,
    project_id=shareable_id,
)
