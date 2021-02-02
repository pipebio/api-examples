from typing import Optional

from library.models.sequence_document_kind import SequenceDocumentKind


class UploadSummary:
    id: int
    sequence_count: Optional[int]
    sequence_document_kind: Optional[SequenceDocumentKind]

    def __init__(self, id: int, sequence_count: int = None, sequence_document_kind: SequenceDocumentKind = None):
        self.id = id
        self.sequence_count = sequence_count
        self.sequence_document_kind = sequence_document_kind

    def __repr__(self):
        return 'UploadSummary({},count={},kind={})'.format(self.id, self.sequence_count, self.sequence_document_kind)

    def to_json(self):
        data = {'visible': True}

        if self.sequence_count is not None:
            data['sequenceCount'] = self.sequence_count
        if self.sequence_document_kind is not None:
            data['sequenceDocumentKind'] = self.sequence_document_kind.value

        return data
