from enum import Enum


class JobType(Enum):
    ImportJob = 'ImportJob'
    FlashJob = 'FlashJob'
    AnnotateJob = 'AnnotateJob'
    CompareJob = 'CompareJob'
    ClusterJob = 'ClusterJob'
    ChartJob = 'ChartJob'
    AlignJob = 'AlignJob'
    SubtractionJob = 'SubtractionJob'
    AddToSequenceStoreJob = 'AddToSequenceStoreJob'
    QuerySequenceStoreJob = 'QuerySequenceStoreJob'
    RemoveSequencesFromStoreJob = 'RemoveSequencesFromStoreJob'
    TreeJob = 'TreeJob'
    ExtractJob = 'ExtractJob'
    ConcatenateJob = 'ConcatenateJob'
    CodonOptimiseJob = 'CodonOptimiseJob'
    ExportJob = 'ExportJob'
    SangerAssemblyJob = 'SangerAssemblyJob'