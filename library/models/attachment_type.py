from enum import Enum


class AttachmentType(Enum):
    fastqSequenceQuality = 'fastqSequenceQuality'
    AminoAcidHeatmap = 'AminoAcidHeatmap'
    LengthDistribution = 'LengthDistribution'
    ClusterLevels = 'ClusterLevels'
    Report = 'Report'
