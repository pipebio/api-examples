import base64
import json
import sys
import traceback
import uuid
from multiprocessing.pool import ThreadPool
from typing import List, Optional
import re

from library.column import Column, IntegerColumn, StringColumn, take_uniques, NumberColumn, ConstantColumn
from library.models.entity_types import EntityTypes
from library.models.render_codes import RendererCodes
from library.models.sequence_document_kind import SequenceDocumentKind
from library.models.table_column_type import TableColumnType
from library.sequences import Sequences
from library.util import Util


class Uploader:
    # Detect if the file is an alignment or not.
    _previous_sequence_length: int
    _sequences_all_same_length: int
    _has_dashes: int
    entity_type: EntityTypes
    sequences: Sequences

    # Row count.
    index: int

    NUMBER_PATTERN_COMPILED = re.compile(r"(\d+\.?/?\d*)+")
    ANNOTATIONS_PATTERN_COMPILED = re.compile(r"^(vk|vl|vh)?[aA]nnotations$")

    def __init__(self,
                 entity_id: int,
                 schema: List[Column],
                 sequences: Sequences,
                 chunk_size=50 * 1000,
                 make_charts=False,
                 entity_type: EntityTypes = EntityTypes.SEQUENCE_DOCUMENT):
        """
        Writes data to a file. Upload the file to db by calling "upload".
        """
        # annotation columns
        self.annotation_column_names = set()
        # sort columns
        self.sort_column_names = set()

        self.sequences = sequences

        self.chunk_size = chunk_size
        # We should not append "_sort" columns for some entries because they just don't need it and would take up
        # space we can't afford (from a performance perspective).
        self.no_sort = Uploader.build_no_sort_cols()
        # Ids for each row.
        self.index = 0
        self.entity_id = entity_id

        # We need to work out from the sequences themselves:
        #   - if alignment or regular document
        #   - and if DNA or AA.
        self.entity_type = entity_type
        self._sequences_all_same_length = True
        self._has_dashes = False
        self._previous_sequence_length = -1

        schema.extend([
            IntegerColumn('id'),
            StringColumn('name'),
            StringColumn('description'),
            StringColumn('labels'),
        ])

        # Object equality on Columns is handled by the Column class equality method.
        unique_schema = take_uniques(schema)
        self.schema = self.add_natural_sort_columns(unique_schema)

        for column in self.schema:
            if Uploader.ANNOTATIONS_PATTERN_COMPILED.match(column.name):
                self.annotation_column_names.add(column.name)

        self.kind = SequenceDocumentKind.DNA
        self.thread_pool = ThreadPool(50)
        self.results = []

        self.file_path = '/tmp/{}_{}.tsv'.format(self.entity_id, uuid.uuid4())
        self.file_writer = open(self.file_path, 'wb')
        self.make_charts = make_charts if make_charts is not None else False

    @staticmethod
    def build_no_sort_cols():
        no_sort = ['nucleotides', 'protein', 'status', 'errors', 'warnings', 'labels', 'mutations', 'hVGene', 'hJGene',
                   'lVGene', 'lJGene', 'targetNT', 'targetAA']
        postfixes = ['annotations', 'sequence', 'quality', 'chromatogram', 'Distribution', 'aabarchart', 'imgtNumbers']
        sequence_cols = []
        for chain in ['', 'vk', 'vl', 'vh']:
            for postfix in postfixes:
                capitalized_postfix = postfix.capitalize() if chain != '' else postfix
                sequence_cols.append(chain + capitalized_postfix)

        no_sort.extend(sequence_cols)
        return no_sort

    def cols_to_header_line(self):
        items = list(map(lambda col: col.name, self.schema))
        return '\t'.join(items) + '\n'

    def write_data(self, data: dict):
        """
        Writes a row by applying the schema.
        """
        line = self.make_line(data)

        # Temp variable required to avoid "Storing unsafe C derivative of temporary Python reference".
        temp_line = line.encode('UTF-8')

        self.file_writer.write(temp_line)

        # Now check first few rows to make some decisions.
        max_no_rows_in_ui = 2000
        if 'sequence' in data and self.index < max_no_rows_in_ui:
            sequence = data['sequence']
            self.kind = Util.get_sequence_kind(sequence)

            sequence_length = len(sequence)
            if '-' in sequence:
                self._has_dashes = True

            if sequence_length != self._previous_sequence_length and self._previous_sequence_length != -1:
                self._sequences_all_same_length = False

            self._previous_sequence_length = sequence_length

        self.upload_if_ready()

    def upload_if_ready(self, force=False):
        # Avoid out of memory errors.
        if self.index % self.chunk_size == 0 or force:
            # Release.. probably not necessary but seems good to do.
            if self.file_writer and not self.file_writer.closed:
                self.file_writer.close()

            # Upload this chunk.
            self.results.append(self.thread_pool.apply_async(self._upload, args=[self.file_path]))
            self.file_path = '/tmp/{}_{}.tsv'.format(self.entity_id, uuid.uuid4())
            # Open the next file, ready to write to it.
            self.file_writer = open(self.file_path, 'wb')

            print('Buffer is now len={}'.format(self.index))

    def get_type(self):
        return EntityTypes.ALIGNMENT if (self._sequences_all_same_length and self._has_dashes) else self.entity_type

    @staticmethod
    def escape_tsv_within_tsv(tsv_line):
        """
        We encode annotations as a tsv within the larger tsv document, therefore we need to escape tsv chartacters.
        :return:
        """
        return str(tsv_line).replace('\t', '\\t').replace('\n', '\\n')

    def make_line(self, row_data: dict) -> str:
        """
        Applies the columns to the row data.
        """
        line = []
        prefix = '' if self.index % self.chunk_size == 0 else '\n'

        # Increment the row index for each line that gets written (immediately to convert to one based).
        self.index = self.index + 1
        # Loop through columns, apply any formatting to each column and write.
        for column in self.schema:

            # Default to empty string.
            value = row_data[column.name] if (column.name in row_data and row_data[column.name] is not None) else ''

            if column.name == 'id' and value == '':
                # For writing sequences we need id set for us however for clustering and so on it will already be set
                # (and is critical we don't overwrite that).
                value = str(self.index)

            elif isinstance(column, ConstantColumn):
                value = column.value

            elif column.kind == TableColumnType.NUMERIC:
                value = NumberColumn.write_for_db(value)

            elif column.name == 'length' and 'sequence' in row_data:
                # This method and pass length in as any other field in row_data.
                # Calculate the length at upload time.
                value = len(row_data['sequence'])

            elif column.name in self.sort_column_names:
                # Then we need to go and get the real value (e.g. the value without _sort appended)
                # and then modify the value so it can be used to sort on.
                replaced = column.name[:-5]
                raw_value = row_data.get(replaced, '')
                kind = Uploader.get_sort_kind(column.description)
                # NOTE! Sort columns MUST be strings.
                value = self.fill_string_sort_cell(str(raw_value), kind)

            elif column.name in self.annotation_column_names:
                # Annotations need special treatment.
                value = self.escape_tsv_within_tsv(str(value))

            line.append(str(value))

        return prefix + '\t'.join(line)

    @staticmethod
    def get_sort_kind(description: Optional[str]) -> RendererCodes:
        return RendererCodes.medianv1 \
            if description is not None and RendererCodes.medianv1.value in description \
            else RendererCodes.natural

    def _upload(self, file_path: str) -> bool:
        """
        Upload the OLD file_path and not self.file_path which will have now moved on to the next chunk.
        """

        try:
            print('entity_id={} Creating signed url'.format(self.entity_id))
            url_response = self.sequences.create_signed_upload(self.entity_id)
            base64_url = url_response['url']
            signed_url = base64.b64decode(base64_url).decode("utf-8")
            file_part_id = url_response['id']
            print('entity_id={} Created signed url, uploading'.format(self.entity_id))
            self.sequences.upload(signed_url, file_path)

            schema = self.build_unique_schema()

            print('entity_id={} Uploaded, importing'.format(self.entity_id))
            self.sequences.import_signed_upload({
                'entityId': self.entity_id,
                'schema': schema,
                'id': file_part_id
            })
            print('entity_id={} Imported'.format(self.entity_id))
            sys.stdout.flush()
            return True
        except Exception as e:
            print('e', e)
            track = traceback.format_exc()
            print(track)
            sys.stdout.flush()
            return False

    def build_unique_schema(self):
        schema = []
        uniques = set()
        for col in self.schema:
            if col.name not in uniques:
                uniques.add(col.name)
                schema.append({
                    'name': col.name,
                    'type': col.type.value,
                    'description': col.description
                })
        return schema

    def upload(self, allow_empty=False) -> bool:
        """
        :param allow_empty: In the case of merged reads, all reads may merge therefore the unmerged files will not have
        any contents (and the reverse is true). Therefore we should not fail the entire job if there is nothing in the
        buffer.
        :return:
        """
        if self.index > 0:
            # Any last bits.
            self.upload_if_ready(True)
            # NOTE:
            # - We are actually doing this threading stuff correctly (I believe). If you find that python is hanging,
            # that's part of python 3.7 sadly. So restart and try again..
            #
            # - Closing first and then joining is fine; closing just means that no more work will be added and then join
            # is to wait for the threads to be done before continuing.
            #
            # - I found that both .join() alone and also with .close() called first worked; so rather than change
            # something that is generally working I'm leaving this as is.
            #
            # Please see these articles:
            # https://bugs.python.org/issue39360
            # https://stackoverflow.com/questions/38271547/when-should-we-call-multiprocessing-pool-join
            self.thread_pool.close()
            self.thread_pool.join()
            results = [r.get() for r in self.results]
            print('results:', results)
            # Return status about whether this went ok or not.
            return False not in results

        elif not allow_empty and self.index == 0:
            # NOTE: The developer can call .upload() many times. so just having an empty buffer does not mean that we
            # have not uploaded everything. Therefore we need to check both empty buffer AND index==0.

            # In the case of merged reads, all reads may merge therefore the unmerged files will not have any contents
            # (and the reverse is true). Therefore we should not fail the entire job if there is nothing in the buffer.
            raise ValueError('No sequences written to result. Was the filter to strict?')
        else:
            return False

    def needs_natural_sort(self, column: Column) -> bool:
        """
        Some columns are blacklisted as not needing sort. Other more uncommon columns can mark they do not need sort
        by setting "column.no_sort".
        """
        return column.type == TableColumnType.STRING \
               and (column.description is None or 'r:label' not in column.description) \
               and not column.name in self.no_sort \
               and not column.no_sort \
               and not (column.name.endswith('Sequence') or column.name.endswith('sequence'))

    def add_natural_sort_columns(self, schema: List[Column]) -> List[Column]:
        """
        Inserts columns for natural sort.
        """

        result = []

        for column in schema:
            result.append(column)

            if self.needs_natural_sort(column):

                sort_name = '{}_sort'.format(column.name)

                if Uploader.get_sort_kind(column.description) == RendererCodes.medianv1:
                    # Sort by medians for boxplots.
                    result.append(Column(sort_name, TableColumnType.NUMERIC, column.description))
                else:
                    # Natural sort by strings.
                    result.append(Column(sort_name, TableColumnType.STRING, column.description))

                self.sort_column_names.add(sort_name)

        return result

    @staticmethod
    def fill_string_sort_cell(value: str, kind: RendererCodes.medianv1 = None):

        if value is None or value == '' or value.isalpha() or value.isnumeric():
            return value

        if kind == RendererCodes.natural or kind is None:
            # Find all series of numbers.
            result = ''
            # Start of "value"
            previous = 0
            # Find all "runs" of numbers.
            matches = Uploader.NUMBER_PATTERN_COMPILED.finditer(value)
            # Replace each number with a zero-padded equivalent.
            for match in matches:
                start = match.start()
                end = match.end()
                # Extract the matched number.
                number = value[start:end]
                # Pad the number so the entire string is 10 characters long.
                replacement = number.zfill(10)
                # Get any characters since the last numeric value.
                string = value[previous:start]
                # Concatenate the previous string, any more characters and the replaced .
                result = f'{result}{string}{replacement}'
                # Save the "end" of the match for the next pass
                previous = end

            # Any trailing characters?
            return result + value[previous:]
        elif kind == RendererCodes.medianv1:
            # Sort by the median value
            if value:
                parsed = json.loads(value)
                # Need to round to max 9dp for db.
                return NumberColumn.write_for_db(parsed[2]) if len(parsed) > 2 else 0
            else:
                return 0
        else:
            raise Exception('Unsupported sort kind:{}'.format(kind))
