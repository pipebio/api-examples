import os
from typing import Any
from urllib.request import URLopener

from requests.sessions import Session

from library.entities import Entities
from library.jobs import Jobs
from library.models.export_format import ExportFormat
from library.models.job_type import JobType
from library.sequences import Sequences
from library.shareables import Shareables


class PipebioClient:
    _session: Session
    shareables: Shareables
    entities: Entities
    jobs: Jobs
    sequences: Sequences
    user: Any

    def __init__(self):
        self._session = Session()
        api_key = os.environ['PIPE_API_KEY'] if 'PIPE_API_KEY' in os.environ else None
        if api_key is None:
            print('PIPE_API_KEY required.')
            print(f'PIPE_EMAIL={api_key}')
            quit()

        # Set Bearer token header with API KEY
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})

        base_url = 'https://app.pipebio.com'
        self.shareables = Shareables(base_url, self._session)
        self.entities = Entities(base_url, self._session)
        self.jobs = Jobs(base_url, self._session)
        self.sequences = Sequences(base_url, self._session)
        self.user = self.get_user(base_url)

    def get_user(self, base_url: str):
        url = f'{base_url}/api/v2/me'
        response = self._session.get(url)
        user = response.json()
        return user

    def upload_file(self,
                    file_name: str,
                    absolute_file_location: str,
                    parent_id: int,
                    project_id: str,
                    organization_id: str):
        print('  Creating signed upload.')
        response = self.jobs.create_signed_upload(
            file_name,
            parent_id,
            project_id,
            organization_id,
        )

        url = response['data']['url']
        job_id = response['data']['job']['id']
        headers = response['data']['headers']

        self.jobs.upload_data_to_signed_url(absolute_file_location, url, headers)
        print('  Upload complete. Parsing contents.')

        return self.jobs.poll_job(job_id)

    def export(self,
               entity_id: int,
               format: ExportFormat,
               destination_folder: str = None):
        entity = self.entities.get(entity_id)
        entity_name = entity['name']
        user = self.user

        path_parts = entity['path'].split('.')
        # Last path part is always the current document.
        # Any before that are ancestor folders, the first being the parent.
        parent_folder_id = int(path_parts[-2]) if len(path_parts) > 1 else None

        job_id = self.jobs.create(
            owner_id=user['orgs'][0]['id'],
            shareable_id=entity['ownerId'],
            job_type=JobType.ExportJob,
            name='Export from python client',
            input_entity_ids=[entity_id],
            params={
                "filter": [],
                "format": format,
                "fileName": entity_name,
                "selection": [],
                "targetFolderId": parent_folder_id,
            }
        )

        # Wait for the file to be converted to Genbank.
        job = self.jobs.poll_job(job_id)

        links = job['outputLinks']

        outputs = []

        for link in links:
            testfile = URLopener()

            destination = os.path.join(destination_folder, entity_name)
            testfile.retrieve(link['url'], destination)

            outputs.append(destination)

        return outputs
