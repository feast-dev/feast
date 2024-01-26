from typing import Optional, Dict, Set, Tuple
from feast import BigQuerySource
from feast.data_source import DataSourceProto


class IcebergSource(BigQuerySource):
    """
    IcebergSource is an extension of BigQuerySource that includes additional parameters specific to Iceberg.
    """

    def __init__(
        self,
        *,
        eventTypes: Optional[Set[str]] = None,
        dateRange: Optional[Tuple[str, str]] = None,
        isStreaming: Optional[bool] = None,
        useEventTimeAligner: Optional[bool] = None,
        **kwargs,
    ):
        """
        Create an IcebergSource from an existing table or query.

        Args:
            eventTypes (optional): Set of event types to be included.
            dateRange (optional): Tuple of start and end dates for the data range.
            isStreaming (optional): Boolean flag indicating if the source is streaming.
            useEventTimeAligner (optional): Boolean flag indicating if event time aligner is used.
            **kwargs: Other arguments inherited from BigQuerySource.
        """
        super().__init__(**kwargs)
        self.eventTypes = eventTypes
        self.dateRange = dateRange
        self.isStreaming = isStreaming
        self.useEventTimeAligner = useEventTimeAligner

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Create an IcebergSource from a protobuf representation.

        Args:
            data_source (DataSourceProto): Protobuf representation of a data source.

        Returns:
            IcebergSource: A new IcebergSource object.
        """
        return IcebergSource(
            eventTypes=set(data_source.iceberg_options.eventTypes),
            dateRange=(data_source.iceberg_options.dateRangeStart, data_source.iceberg_options.dateRangeEnd),
            isStreaming=data_source.iceberg_options.isStreaming,
            useEventTimeAligner=data_source.iceberg_options.useEventTimeAligner,
            **BigQuerySource.from_proto(data_source)
        )

    def to_proto(self) -> DataSourceProto:
        """
        Convert the IcebergSource to its protobuf representation.

        Returns:
            DataSourceProto: Protobuf representation of the IcebergSource.
        """
        data_source_proto = super().to_proto()
        data_source_proto.iceberg_options.eventTypes.extend(self.eventTypes)
        data_source_proto.iceberg_options.dateRangeStart, data_source_proto.iceberg_options.dateRangeEnd = self.dateRange
        data_source_proto.iceberg_options.isStreaming = self.isStreaming
        data_source_proto.iceberg_options.useEventTimeAligner = self.useEventTimeAligner

        return data_source_proto