import gzip
import os
import re
import unittest

from example_01_upload_example import example_01_upload_example
from example_02_download_example import example_02a_download_result_as_tsv, \
    example_02b_download_result_to_memory_to_do_more_work, example_02c_download_result_to_biological_format, \
    example_02d_download_original_file
from example_03_cluster_example import example_03_cluster_example


class ExampleE2ETests(unittest.TestCase):

    def test_example_01_uploads_an_example_file(self):
        """
        Upload a file on disk to PipeBio
        """
        job = example_01_upload_example()

        # Make some basic assertions to ensure the job ran to completion.
        self.assertEqual(job['type'], 'ImportJob')
        self.assertEqual(job['name'], '137_adimab_VL.fsa')

        # Log the newly created document.
        # You might like to do something with this document id, such as run a QC or Annotation job on it.
        output_entities = job['outputEntities']
        print('Result document id & name: ', str(output_entities[0]))

    def test_example_02a_downloads_to_tsv(self):
        """
        Download a PipeBio file to disk in raw tsv format.
        """
        result = example_02a_download_result_as_tsv(296716)

        contents = self.read(result)
        lines = contents.split('\n')

        # Check header and first line.
        header = lines[0]
        expected_header = 'id\tname\tname_sort\tdescription\tdescription_sort\tsequence\tlength\tannotations\tquality\tstatus\terrors\twarnings\tmutations\treversed\tChain\tChain_sort\thVGene\thVGene_sort\thVGenePercIdentity\thJGene\thJGene_sort\thJGenePercIdentity\tigghNtSequence\tigghNtLength\tigghAaSequence\tigghAaLength\tfrh4Sequence\tfrh4Length\tcdrh3Sequence\tcdrh3Length\tfrh3Sequence\tfrh3Length\tcdrh2Sequence\tcdrh2Length\tfrh2Sequence\tfrh2Length\tcdrh1Sequence\tcdrh1Length\tfrh1Sequence\tfrh1Length\tchromatogram'
        self.assertEqual(header, expected_header)

        first_line = lines[1]
        expected_first_line = '1\tP00863_C03\tP0000000863_C0000000003\tcrenezumab\tcrenezumab\tGAGGTGCAGCTGGTGGAGAGCGGCGGCGGCCTGGTGCAGCCCGGCGGCAGCCTGAGGCTGAGCTGCGCCGCCAGCGGCTTCACCTTCAGCAGCTACGGCATGAGCTGGGTGAGGCAGGCCCCCGGCAAGGGCCTGGAGCTGGTGGCCAGCATCAACAGCAACGGCGGCAGCACCTACTACCCCGACAGCGTGAAGGGCAGGTTCACCATCAGCAGGGACAACGCCAAGAACAGCCTGTACCTGCAGATGAACAGCCTGAGGGCCGAGGACACCGCCGTGTACTACTGCGCCAGCGGCGACTACTGGGGCCAGGGCACCACCGTGACCGTGAGCAGC\t336\ttype\\tstart\\tend\\tlabel\\nFR\\t1\\t75\\tFR-H1\\nFR\\t106\\t147\\tFR-H2\\nFR\\t178\\t294\\tFR-H3\\nFR\\t304\\t336\\tFR-H4\\ngene\\t2\\t291\\tIGHV3-2*01\\ngene\\t298\\t318\\tIGHJ4*01\\nMutation\\t1\\t3\\tQ1E\\nMutation\\t139\\t141\\tS47L\\nMutation\\t145\\t147\\tS49A\\nMutation\\t181\\t183\\tA61P\\nMutation\\t232\\t234\\tT78S\\nMutation\\t259\\t261\\tK87R\\nMutation\\t262\\t264\\tP88A\\nMutation\\t319\\t321\\tQ10T\\nCDR\\t76\\t105\\tCDR-H1\\nCDR\\t148\\t177\\tCDR-H2\\nCDR\\t295\\t303\\tCDR-H3\\nCDS\\t1\\t336\\tIgG-H\\nWarning\\t157\\t165\\tDeamidation\\n\t\tCORRECT\t\tDeamidation\tQ1E, S47L, S49A, A61P, T78S, K87R, P88A, Q10T\tfalse\tvh\tvh\tIGHV3-2*01\tIGHV0000000003-0000000002*0000000001\t76.207\tIGHJ4*01\tIGHJ0000000004*0000000001\t95.238\tGAGGTGCAGCTGGTGGAGAGCGGCGGCGGCCTGGTGCAGCCCGGCGGCAGCCTGAGGCTGAGCTGCGCCGCCAGCGGCTTCACCTTCAGCAGCTACGGCATGAGCTGGGTGAGGCAGGCCCCCGGCAAGGGCCTGGAGCTGGTGGCCAGCATCAACAGCAACGGCGGCAGCACCTACTACCCCGACAGCGTGAAGGGCAGGTTCACCATCAGCAGGGACAACGCCAAGAACAGCCTGTACCTGCAGATGAACAGCCTGAGGGCCGAGGACACCGCCGTGTACTACTGCGCCAGCGGCGACTACTGGGGCCAGGGCACCACCGTGACCGTGAGCAGC\t336\tEVQLVESGGGLVQPGGSLRLSCAASGFTFSSYGMSWVRQAPGKGLELVASINSNGGSTYYPDSVKGRFTISRDNAKNSLYLQMNSLRAEDTAVYYCASGDYWGQGTTVTVSS\t112\tWGQGTTVTVSS\t11\tGDY\t3\tYPDSVKGRFTISRDNAKNSLYLQMNSLRAEDTAVYYCAS\t39\tSINSNGGSTY\t10\tWVRQAPGKGLELVA\t14\tGFTFSSYGMS\t10\tEVQLVESGGGLVQPGGSLRLSCAAS\t25\t'
        self.assertEqual(first_line, expected_first_line)

    def test_example_02b_downloads_to_memory(self):
        """
        Download a PipeBio file to disk to memory in Python so we can process it.
        """
        result = example_02b_download_result_to_memory_to_do_more_work(296717)

        # The result is a map of id to row data.
        # To get the row values we take the values and sort by name.
        in_memory_rows = sorted(list(result.values()), key=lambda row: row['name'])

        # Here you can get the name, sequence, annotations and all the other properties.
        self.assertEqual(in_memory_rows[0]['name'], 'P00863_C03')

    def test_example_02c_downloads_results_to_genbank_format(self):
        """
        Download a PipeBio file and specify the result format. In this case we choose Genbank format.
        """
        result = example_02c_download_result_to_biological_format(296717)
        # NOTE: The date is intentionally modified below so that our tests will pass, even as time marches on.
        expected_genbank = '''
LOCUS       P00863_C03               112 aa                     UNK XX-XXX-XXXX
DEFINITION  crenezumab.
ACCESSION   1
VERSION     1
KEYWORDS    .
SOURCE      .
  ORGANISM  .
            .
FEATURES             Location/Qualifiers
     FR              1..25
                     /label="FR-H1"
     FR              36..49
                     /label="FR-H2"
     FR              60..98
                     /label="FR-H3"
     FR              102..112
                     /label="FR-H4"
     gene            1..97
                     /label="IGHV3-2*01"
     gene            100..106
                     /label="IGHJ4*01"
     Mutation        1
                     /label="Q1E"
     Mutation        47
                     /label="S47L"
     Mutation        49
                     /label="S49A"
     Mutation        61
                     /label="A61P"
     Mutation        78
                     /label="T78S"
     Mutation        87
                     /label="K87R"
     Mutation        88
                     /label="P88A"
     Mutation        107
                     /label="Q10T"
     CDR             26..35
                     /label="CDR-H1"
     CDR             50..59
                     /label="CDR-H2"
     CDR             99..101
                     /label="CDR-H3"
     CDS             1..112
                     /label="IgG-H"
     Warning         53..55
                     /label="Deamidation"
ORIGIN
        1 evqlvesggg lvqpggslrl scaasgftfs sygmswvrqa pgkglelvas insnggstyy
       61 pdsvkgrfti srdnaknsly lqmnslraed tavyycasgd ywgqgttvtv ss
//'''.strip()
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
        absolute_location = example_02d_download_original_file(296713, destination_filename)

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

        document_to_cluster = 296716
        result_folder_id = 296712

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
