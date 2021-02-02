import os
from typing import Dict, Any, Union
from urllib.request import URLopener

from requests.sessions import Session

from library.authentication import Authentication
from library.entities import Entities
from library.jobs import Jobs
from library.models.export_format import ExportFormat
from library.models.job_type import JobType
from library.sequences import Sequences
from library.shareables import Shareables


class PipebioClient:
    _session: Session
    authentication: Authentication
    shareables: Shareables
    entities: Entities
    jobs: Jobs
    sequences: Sequences

    def __init__(self):
        self._session = Session()
        base_url = 'https://app.pipebio.com'
        self.authentication = Authentication(base_url)
        self.shareables = Shareables(base_url, self._session)
        self.entities = Entities(base_url, self._session)
        self.jobs = Jobs(base_url, self._session)
        self.sequences = Sequences(base_url, self._session)

    def login(self, email: str = None, password: str = None, token: str = None) -> None:
        """
        Login with arguments or by specifying environment variables.
        """
        response = self.authentication.login(email, password, token)

        response_session = response['session']
        self._session.__dict__.update(response_session.__dict__)
        self._user = response['user']

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
        user = self.authentication.user

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
                "targetFolderId": entity['path'].split('.')[-2],
            }
        )

        # Wait for the file to be converted to Genbank.
        job = self.jobs.poll_job(job_id)

        links = job['outputLinks']

        for link in links:
            testfile = URLopener()

            destination = os.path.join(destination_folder, entity_name)
            testfile.retrieve(link['url'], destination)
