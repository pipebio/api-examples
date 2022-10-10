from enum import Enum


class EntityTypes(Enum):
    FOLDER = 'FOLDER'
    REPORT = 'REPORT'
    CLUSTER = 'CLUSTER'
    CLUSTER_V3 = 'CLUSTER_V3'
    COMPARISON = 'COMPARISON'
    SEQUENCE_DOCUMENT = 'SEQUENCE_DOCUMENT'
    ALIGNMENT = 'ALIGNMENT'
    IMAGE = 'IMAGE'
    PDF = 'PDF'

    def __str__(self):
        return '%s' % self.value
