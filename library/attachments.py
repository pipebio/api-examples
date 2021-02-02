from typing import Union, List

import requests

from library.models.attachment_type import AttachmentType
from library.util import Util


class Attachments:
    url: str
    session: requests.sessions

    def __init__(self, url: str, session: requests.sessions):
        self._url = url
        self.url = '{0}/api/v2/entities'.format(url)
        self.session = session

    def create(self, entity_id: int, type: AttachmentType, data: Union[dict, List]):
        print('Creating attachment: entity_id={},kind={}'.format(entity_id, type.value))
        url = '{}/{}/attachments'.format(self.url, entity_id)
        json = {"data": data, "type": type.value}
        response = self.session.post(url, json=json)
        Util.raise_detailed_error(response)
        print('Creating attachment: response', response.status_code)
        return response.json()

    def upsert(self, entity_id: int, type: AttachmentType, data: Union[dict, List], version: int = 1,
               ignore_version=True):
        """
        Create or update if exists.
        """
        print('Upserting attachment: entity_id={},type={},version={},ignore_version={}'.format(entity_id, type.value,
                                                                                               version,
                                                                                               ignore_version))
        url = '{}/{}/attachments'.format(self.url, entity_id)
        json = {"data": data, "version": version, "type": type.value, "ignoreVersion": ignore_version}
        response = self.session.put(url, json=json)
        Util.raise_detailed_error(response)
        print('Creating attachment: response', response.status_code)

    def get(self, entity_id: str, type: AttachmentType, version: int = 1):
        url = '{}/{}/attachments/{}'.format(self.url, entity_id, type.value, json={'version': version})
        response = self.session.get(url)
        Util.raise_detailed_error(response)
        return response.json()
