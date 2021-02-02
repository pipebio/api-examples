from enum import Enum


class EntityTypes(Enum):

    FOLDER = 'FOLDER'
    # Editable report document.
    REPORT = 'REPORT'

    # Old style cluster docs.
    CLUSTER = 'CLUSTER'
    # These are at the time of writing using the same viewer / the same.
    CLUSTER_V3 = 'CLUSTER_V3'
    COMPARISON = 'COMPARISON'

    # Scalable document without grouping.
    SEQUENCE_DOCUMENT = 'SEQUENCE_DOCUMENT'
    # Scalable document without grouping. Should be moved to postgres.
    ALIGNMENT = 'ALIGNMENT'

    def __str__(self):
        return '%s' % self.value
