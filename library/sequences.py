import csv
import gzip
import os
import tempfile
from multiprocessing.pool import ThreadPool
from typing import List, Dict
from urllib.request import URLopener

import requests

from library.column import Column
from library.entities import Entities
from library.models.sort import Sort
from library.models.table_column_type import TableColumnType
from library.util import Util


class Sequences:
    # Static property used to join entity_id with sequence_id.
    merge_delimiter = '##@##'

    def __init__(self, url: str, session: requests.sessions):
        self._url = url
        self.url = '{0}/api/v2'.format(url)
        self.session = Util.mount_standard_session(session)
        self.entities = Entities(url, session)

    def parallel_download(self, entity_ids: List[int]) -> None:
        print('Download starting')
        entities = {}

        def download(entity_id: int) -> None:
            entities[entity_id] = self.download(entity_id, Sequences.get_filepath_for_entity_id(entity_id))

        list(ThreadPool(8).imap_unordered(download, entity_ids))

    def download_to_memory(self, entity_ids: List[int]):
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

        for entity_id in entity_ids:
            sequence_map = self.read_tsv_to_map(
                Sequences.get_filepath_for_entity_id(entity_id),
                str(entity_id),
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

    def download(self, entity_id: int, destination: str = None, sort: List[Sort] = None) -> str:
        """
        Download sequences from a single entity.
        """

        sort = [Sort('id', 'asc')] if sort is None else sort
        sort = list(sort_item.to_json() for sort_item in sort) if sort else []
        body = {'filter': [], 'selection': [], 'sort': sort}
        file_path = Sequences.get_filepath_for_entity_id(entity_id)
        url = '{}/entities/{}/_extract'.format(self.url, entity_id)
        print('Downloading shards from "{}" to "{}".'.format(url, file_path))

        paths = []
        with self.session.post(url, stream=True, timeout=10 * 60, json=body) as response:
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

        sorted_paths = self.get_sorted_file_shard_list(entity_id, paths, [])

        print(f'Unzipping: entity_id={entity_id} to destination={destination}')

        skip_first = False

        with open(destination, 'wb+') as target_file:
            for file_shard in sorted_paths:
                with gzip.open(file_shard, 'rb') as g_zip_file:
                    first_line = True
                    for line in g_zip_file:
                        # We skip the first line of every file, except for the very first.
                        if not (first_line and skip_first):
                            line = Sequences.sanitize(line.decode("utf-8"))
                            target_file.write(line.encode("utf-8"))
                        first_line = False
                # We skip the first line of every file, except for the very first.
                skip_first = True

        return destination

    @staticmethod
    def sanitize(line: str) -> str:
        if '"' not in line:
            return line
        else:
            sanitized_line = []
            ending = "\n" if line.endswith("\n") else ""
            splits = line.rstrip("\n").split("\t")
            for split in splits:
                if not split.startswith('"'):
                    sanitized_line.append(split)
                else:
                    sanitized_line.append(split[1:-1].replace('""', '"'))
        return '\t'.join(sanitized_line) + ending

    @staticmethod
    def get_filepath_for_entity_id(entity_id: any, extension='tsv'):
        file_name = '{}.{}'.format(entity_id, extension)
        return os.path.join(tempfile.gettempdir(), file_name)

    def get_sorted_file_shard_list(self, entity_id: int, file_shard_list: List[str], sort: list):
        """
        Sorts the file_shard_list to ensure that the shards can be stitched back together in the correct order
        This is needed as the response 'chunks' are not necessarily named in the correct order.

        :param entity_id: - document to download
        :param file_shard_list: List[str] - All of the file names of the shards
        :param sort: List[Sort] - list of sorts applied, processed in order, same way SQL does, so order matters
        :return:  List[str] - All of the file names of the shards ordered by the sort
        """

        if sort is None or len(sort) == 0:
            return file_shard_list

        all_fields = self.entities.get_fields(entity_id=entity_id)

        shard_first_data_lines = []

        # get values of sort columns for first data line of each shard
        for file_shard in file_shard_list:
            with gzip.open(file_shard, 'rt') as g_zip_file:
                tsv_reader = csv.reader(g_zip_file, delimiter="\t")
                lines = 2
                header = None
                file_details = {'file_shard': file_shard}

                # reads the first line and headers of each files and pull out
                # all the values we need to sort on
                for i in range(lines):
                    row = next(tsv_reader)
                    if i == 0:
                        header = row
                    else:
                        for sort_column in sort:
                            col_id = sort_column.col_id
                            field = [x for x in all_fields if x.name == col_id][0]
                            col_index = header.index(col_id)
                            # Column.parse returns None for empty string INTEGER/NUMERIC columns,
                            # ideally would change that, but consequences unclear
                            # so overriding that to 0, otherwise take Column.parse output
                            parsed_value = float('-inf') \
                                if (field.kind == TableColumnType.INTEGER or field.kind == TableColumnType.NUMERIC) \
                                   and row[col_index] == '' \
                                else field.parse(row[col_index])
                            file_details[col_id] = parsed_value

            shard_first_data_lines.append(file_details)

        sorted_shard_first_lines = []
        # sort the shards, in reverse order, so last one done is primary sort
        sort.reverse()
        for column_to_sort in sort:
            sorted_shard_first_lines = sorted(shard_first_data_lines,
                                              key=lambda x: x[column_to_sort.col_id],
                                              reverse=column_to_sort.sort == 'desc')

        return list(map(lambda x: x['file_shard'], sorted_shard_first_lines))
