import gzip
import os
import re
import unittest
import tempfile

from example_01_upload_example import example_01a_upload_example_fasta, example_01b_upload_example_tsv
from example_02_download_example import example_02a_download_result_as_tsv, \
    example_02b_download_result_to_memory_to_do_more_work, example_02c_download_result_to_biological_format, \
    example_02d_download_original_file
from example_03_cluster_example import example_03_cluster_example


class ExampleE2ETests(unittest.TestCase):

    def test_example_01a_uploads_an_example_fasta_file(self):
        """
        Upload a file on disk to PipeBio
        """
        job = example_01a_upload_example_fasta()

        # Make some basic assertions to ensure the job ran to completion.
        self.assertEqual(job['type'], 'ImportJob')
        self.assertEqual(job['name'], '137_adimab_VL.fsa')

        # Log the newly created document.
        # You might like to do something with this document id, such as run a QC or Annotation job on it.
        output_entities = job['outputEntities']
        print('Result document id & name: ', str(output_entities[0]))

    def test_example_01b_upload_example_tsv_file(self):
        entity = example_01b_upload_example_tsv()
        print(f'Created document id: {entity.id}')

    def test_example_02a_downloads_to_tsv(self):
        """
        Download a PipeBio file to disk in raw tsv format.
        """
        result = example_02a_download_result_as_tsv(1063911, '1063911_download', tempfile.gettempdir())

        contents = self.read(result)
        lines = contents.split('\n')

        # Check header and first line.
        header = lines[0]
        print(f'header: {header}')
        expected_header = 'id\tname\tdescription\tsequence\tlength\tlabels\tannotations\tTargetTg\tSpecies\tSeqType\tTarget\tcp_test\toriginalName'
        self.assertEqual(header, expected_header)

        first_line = lines[1]
        print(f'first_line {first_line}')
        expected_first_line = '1\tP00125_A01 abituzumab\t\tCAGGTGCAGCTGCAGCAGAGCGGCGGCGAGCTGGCCAAGCCCGGCGCCAGCGTGAAGGTGAGCTGCAAGGCCAGCGGCTACACCTTCAGCAGCTTCTGGATGCACTGGGTGAGGCAGGCCCCCGGCCAGGGCCTGGAGTGGATCGGCTACATCAACCCCAGGAGCGGCTACACCGAGTACAACGAGATCTTCAGGGACAAGGCCACCATGACCACCGACACCAGCACCAGCACCGCCTACATGGAGCTGAGCAGCCTGAGGAGCGAGGACACCGCCGTGTACTACTGCGCCAGCTTCCTGGGCAGGGGCGCCATGGACTACTGGGGCCAGGGCACCACCGTGACCGTGAGCAGC\t354\t\t\t2\thuman\tdedde\tded\ta,b,c\tP00125_A01 abituzumab'
        self.assertEqual(first_line, expected_first_line)

    def test_example_02b_downloads_to_memory(self):
        """
        Download a PipeBio file to disk to memory in Python so we can process it.
        """
        result = example_02b_download_result_to_memory_to_do_more_work(1063911)

        # The result is a map of id to row data.
        # To get the row values we take the values and sort by name.
        in_memory_rows = sorted(list(result.values()), key=lambda row: row['name'])

        # Here you can get the name, sequence, annotations and all the other properties.
        self.assertEqual(in_memory_rows[0]['name'], 'P00125_A01 abituzumab')

    def test_example_02c_downloads_results_to_genbank_format(self):
        """
        Download a PipeBio file and specify the result format. In this case we choose Genbank format.
        """
        result = example_02c_download_result_to_biological_format(1085921, tempfile.gettempdir())
        # NOTE: The date is intentionally modified below so that our tests will pass, even as time marches on.
        expected_genbank = '''
LOCUS       P00125_A01abituz         354 bp    DNA              UNK XX-XXX-XXXX
DEFINITION  .
ACCESSION   P00125_A01abituz
VERSION     P00125_A01abituz
KEYWORDS    .
SOURCE      .
  ORGANISM  .
            .
FEATURES             Location/Qualifiers
ORIGIN
        1 caggtgcagc tgcagcagag cggcggcgag ctggccaagc ccggcgccag cgtgaaggtg
       61 agctgcaagg ccagcggcta caccttcagc agcttctgga tgcactgggt gaggcaggcc
      121 cccggccagg gcctggagtg gatcggctac atcaacccca ggagcggcta caccgagtac
      181 aacgagatct tcagggacaa ggccaccatg accaccgaca ccagcaccag caccgcctac
      241 atggagctga gcagcctgag gagcgaggac accgccgtgt actactgcgc cagcttcctg
      301 ggcaggggcg ccatggacta ctggggccag ggcaccaccg tgaccgtgag cagc
//
'''.strip()
        with open(result[0], 'rt') as handle:
            actual_genbank = handle.read()
            # Replace the date; just to make the test pass.
            actual_genbank = re.sub(r'([0-9]+-[A-Za-z]+-[0-9]+)', 'XX-XXX-XXXX', actual_genbank.strip())

            # Files should match perfectly.
            self.assertEqual(actual_genbank, expected_genbank.strip())

    def test_example_02d_downloads_the_original_file_to_disk(self):
        """
        Download the original, un-parsed file from PipeBio.
        e.g. this is the file the user originally uploaded, byte for byte.
        """
        destination_filename = 'output.fsa'
        absolute_location = example_02d_download_original_file(1085969, destination_filename, tempfile.gettempdir())

        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, './sample_data/adimab/137_adimab_VH.fsa')

        with open(filename, 'rt') as expected_handle:
            expected = expected_handle.read()

            with open(absolute_location, 'rt') as reference:
                actual = reference.read()

                # The downloaded original file should exactly match the version on disk that we started with.
                self.assertEqual(actual.strip(), expected.strip())

    def test_example_03_clusters_an_annotated_document(self):
        """
        Example showing how to run an arbitrary job on a document within PipeBio.
        """

        document_to_cluster = 1085851
        result_folder_id = 1085819

        job = example_03_cluster_example(document_to_cluster, result_folder_id)

        # Here we have a reference to the created document.
        # If we wanted to, we could download the document and look at the contents
        # or perhaps run another job on the results, such as an alignment.
        self.assertEqual(job['type'], 'ClusterJob')
        self.assertEqual(job['name'], 'Cluster job from python client')
        self.assertEqual(job['status'], 'COMPLETE')

    def unzip(self, input_path: str) -> str:
        with gzip.open(input_path, 'rb') as f:
            return f.read()

    def read(self, input_path: str) -> str:
        with open(input_path, 'rt') as f:
            return f.read()
