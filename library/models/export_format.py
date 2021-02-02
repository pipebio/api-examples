from enum import Enum


class ExportFormat(Enum):
    FASTA = 'FASTA'
    FASTQ = 'FASTQ'
    GENBANK = 'GENBANK'
    TSV = 'TSV'
