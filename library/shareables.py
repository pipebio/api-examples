import csv
import json
from io import StringIO
from typing import List

import requests

from library.util import Util


class Shareables:

    def __init__(self, url: str, session: requests.sessions):
        self.url = url
        self.session = session

    def list(self) -> List[dict]:
        url = '{}/api/v2/shareables'.format(self.url)

        response = self.session.get(url)

        print('ShareablesService:list - response:' + str(response.status_code))

        Util.raise_detailed_error(response)

        return response.json()['data']

    def create(self, name: str) -> dict:
        url = '{}/api/v2/shareables'.format(self.url)

        response = self.session.post(url, data=json.dumps({'name': name}), )

        print('ShareablesService:list - response:' + str(response.status_code))

        Util.raise_detailed_error(response)

        return response.json()

    def list_entities(self, shareable_id: str):
        url = '{}/api/v2/shareables/{}/entities'.format(self.url, shareable_id)

        response = self.session.get(url)

        print('ShareablesService:list_entities - response:' + str(response.status_code))

        Util.raise_detailed_error(response)

        file = StringIO(response.text)
        reader = csv.DictReader(file, dialect='excel-tab')
        rows = []
        for row in reader:
            rows.append(row)
        return rows

