import json
from multiprocessing.pool import ThreadPool
from typing import List

import requests

from library.attachments import Attachments
from library.column import Column
from library.models import EntityTypes
from library.models.table_column_type import TableColumnType
from library.models.upload_summary import UploadSummary
from library.util import Util


class Entities:
    url: str
    session: requests.sessions
    attachments_service: Attachments

    def __init__(self, url: str, session: requests.sessions):
        self.url = url
        self.session = session
        self.attachments_service = Attachments(url, session)

    def create_file(self,
                    project_id: str,
                    parent_id: int,
                    name: str,
                    entity_type: EntityTypes = EntityTypes.SEQUENCE_DOCUMENT,
                    visible=False) -> dict:
        print('create_file for parent_id:' + str(parent_id) + ' name:' + str(name))

        payload = {
            'name': name,
            'type': entity_type.value,
            'visible': visible,
            'shareableId': project_id,
        }

        if parent_id is not None:
            payload['parentId'] = int(str(parent_id))

        response = self.session.post(
            '{}/api/v2/entities'.format(self.url),
            headers={'Content-type': 'Application/json'},
            data=json.dumps(payload),
        )
        print('create_file response:' + str(response.status_code))
        Util.raise_detailed_error(response)
        return response.json()

    def create_folder(self, project_id: str, name: str, parent_id: int = None, visible=False):
        return self.create_file(project_id, parent_id, name, EntityTypes.FOLDER, visible=visible)

    def mark_file_visible(self, entity_summary: UploadSummary):
        print('marking visible:', entity_summary)
        response = self.session.patch(
            '{}/api/v2/entities/{}'.format(self.url, entity_summary.id),
            headers={'Content-type': 'Application/json'},
            data=json.dumps(entity_summary.to_json()),
        )
        print('mark_file_visible response:' + str(response.status_code))
        print('mark_file_visible text    :' + str(response.text))
        Util.raise_detailed_error(response)
        return response.json()

    def get(self, entity_id):
        response = self.session.get(
            '{}/api/v2/entities/{}'.format(self.url, entity_id),
            headers={'Content-type': 'Application/json'},
        )
        print('get response:' + str(response.status_code))
        Util.raise_detailed_error(response)
        return response.json()

    def get_all(self, entity_ids):
        results = list(ThreadPool(8).imap_unordered(lambda entity_id: self.get(entity_id), entity_ids))
        for result in results:
            print(result)
        return results

    @staticmethod
    def merge_fields(schemaA: List[Column], schemaB: List[Column]) -> List[Column]:
        result = []
        result.extend(schemaA)

        print('result', result)
        for column in schemaB:
            found = next((col for col in result if col.name == column.name), None)
            print('found', found)
            if found is None:
                result.append(column)

        print('result', result)
        return result

    def get_fields_for_all_entities(self, entity_ids: List[int]) -> List[Column]:
        schema = []
        for entity_id in entity_ids:
            new_schema = self.get_fields(entity_id)
            schema = EntityService.merge_fields(schema, new_schema)
        return schema

    def get_fields(self, entity_id: int, ignore_id=False) -> List[Column]:
        """
        Returns the fields for a document or 404 if there are no fields (e.g. it's a folder).
        :return:
        """
        response = self.session.get(
            '{}/api/v2/entities/{}/fields'.format(self.url, entity_id),
        )
        Util.raise_detailed_error(response)
        columns = []
        for field in response.json():

            if ignore_id and field == 'id':
                continue
            else:
                # Not all columns have field so we need to check it's set.
                description = field['description'] if 'description' in field else None
                columns.append(Column(field['name'], TableColumnType[field['type']], description))

        return columns

    def download_original_file(self, entity_id: int, destination_filename: str) -> str:
        """
        Download the originally uploaded file corresponding to a PipeBio document.
        Two requests are made:
        1. Request a signed url for this document (GET /api/v2/entities/:id/original)
        2. Download the data from that signed url (GET <result-from-step-1>)
        """
        # First request a signed url from PipeBio.
        signed_url_response = self.session.get(
            '{}/api/v2/entities/{}/original'.format(self.url, entity_id),
        )

        # Did the signed-url request work ok?
        Util.raise_detailed_error(signed_url_response)

        # Parse the results to get the signed url.
        download_url = signed_url_response.json()['url']

        # Download the original file.
        download_response = requests.get(download_url)

        # Did the download request work ok?
        Util.raise_detailed_error(download_response)

        # Write the result to disk in chunks.
        with open(destination_filename, 'wb') as f:
            for chunk in download_response.iter_content(chunk_size=8192):
                f.write(chunk)

        return destination_filename