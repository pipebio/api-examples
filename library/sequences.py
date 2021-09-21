import csv
import gzip
import os
import tempfile
from multiprocessing.pool import ThreadPool
from typing import List, Dict
from urllib.request import URLopener

import requests

from library.column import Column
from library.models.table_column_type import TableColumnType
from library.util import Util


class Sequences:

    # Static property used to join entity_id with sequence_id.
    merge_delimiter = '##@##'

    def __init__(self, url: str, session: requests.sessions):
        self._url = url
        self.url = '{0}/api/v2'.format(url)
        self.session = Util.mount_standard_session(session)

    def parallel_download(self, entity_ids: List[int]) -> None:
        print('Download starting')
        entities = {}

        def download(entity_id: int) -> None:
            entities[entity_id] = self.download(entity_id)

        list(ThreadPool(8).imap_unordered(download, entity_ids))

        for entity_id in entity_ids:
            file_shard_list = entities[entity_id]

            print('Unzipping: ' + str(entity_id))
            file_location = Sequences.get_filepath_for_entity_id(entity_id)
            print('file_location', file_location)

            skip_first = False

            with open(file_location, 'wb+') as target_file:
                for file_shard in file_shard_list:
                    with gzip.open(file_shard, 'rb') as g_zip_file:
                        first_line = True
                        for line in g_zip_file:
                            # We skip the first line of every file, except for the very first.
                            if not (first_line and skip_first):
                                target_file.write(line)
                            first_line = False
                    # We skip the first line of every file, except for the very first.
                    skip_first = True

    def download_to_memory(self,entity_ids: List[int]):
        self.parallel_download(entity_ids)

        # Build an in memory map that matches this tsv.
        sequence_map = {}

        columns = [
            Column('id', TableColumnType.STRING),
            Column('name', TableColumnType.STRING),
            Column('sequence', TableColumnType.STRING),
            Column('annotations', TableColumnType.STRING),
            Column('type', TableColumnType.STRING),
        ]

        for bigquery_id in entity_ids:
            sequence_map = self.read_tsv_to_map(
                Sequences.get_filepath_for_entity_id(bigquery_id),
                str(bigquery_id),
                columns,
                sequence_map
            )

        return sequence_map

    def read_tsv_to_map(self,
                        filepath: str,
                        id_prefix: str,
                        columns: List[Column],
                        sequence_map: Dict[str, any] = None) -> Dict[str, any]:

        sequence_map = {} if sequence_map is None else sequence_map

        print('read_tsv_to_map::Reading filepath: "{}"'.format(filepath))
        # Read the file.
        with open(filepath, 'r') as tsvfile:

            replaced = (x.replace('\0', '') for x in tsvfile)
            reader = csv.DictReader(replaced, dialect='excel-tab')

            for row in reader:
                if 'id' not in row:
                    raise Exception('id not in row')

                row_id = int(row['id'])

                compound_id = "{}{}{}".format(id_prefix, Sequences.merge_delimiter, row_id)
                parsed = {}
                for column in columns:
                    name = column.name
                    # Avoid errors like "KeyError: 'type'".
                    parsed[column.name] = column.parse(row[name]) if name in row else column.parse('')

                sequence_map[compound_id] = parsed

        return sequence_map

    def download(self, entity_id: int, destination: str = None) -> List[str]:
        """
        Download sequences from a single entity.

        :param entity_id:
        :param destination:
        :return: array of paths to downloaded files.
        """

        file_path = destination if destination is not None else Sequences.get_filepath_for_entity_id(entity_id)
        url = '{}/entities/{}/_extract?sort=id'.format(self.url, entity_id)
        print('downloading "{}" to "{}".'.format(url, file_path))

        paths = []
        with self.session.post(url, stream=True, timeout=10 * 60) as response:
            try:
                links = response.json()
                print('links', links)
                if 'statusCode' in links and links['statusCode'] != 200:
                    raise Exception(links['message'])
                elif len(links) == 0:
                    raise Exception(
                        'Sequences:download - Error; no download links for {}. Does the table exist?'.format(entity_id))

                index = 0
                for link in links:
                    testfile = URLopener()
                    path = '{}-{}.gz'.format(file_path, index)
                    paths.append(path)
                    testfile.retrieve(link, path)
                    index = index + 1
            except Exception as e:
                print('Sequences:download - error:', e)
                raise e

        return paths

    @staticmethod
    def get_filepath_for_entity_id(entity_id: any, extension='tsv'):
        file_name = '{}.{}'.format(entity_id, extension)
        return os.path.join(tempfile.gettempdir(), file_name)
