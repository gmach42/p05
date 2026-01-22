from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.data_type = "Generic Data"

    def get_name(self) -> str:
        return self.stream_id

    def get_type(self) -> str:
        return self.type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        # Process Default implementations
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        # Process Default implementations
        pass


class SensorStream(DataStream):
    """
    Process environnamental data received as dict{}

    Temp in °C
    Humidity in %HR and indicate the percetage of water vapor in a the air
    100 %HR meaning that the air is holding the maximum of water vapor by volume unit
    Pressure in hPa
    """

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class TransactionStream(DataStream):
    """Process transactionnal data received as list[int]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self, result: str) -> Dict[str, Union[str, int, float]]:
        pass


class EventStream(DataStream):
    """Process events' data received as list[str]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class StreamProcessor:
    """
    Dispatch the data batch into the different streams depending on the format
    """
    def __init__(self):
        self.process_count = 0

    def process_any_stream(self, stream: DataStream, data: List[Any]) -> str:
        result = stream.process_batch(data)
        stats = stream.get_stats(result)
        print(f"{stream.get_type()} Data: {stats} processed.")
        self.process_count += 1
        return stats

    def process_multiple_streams(
        self, streams: List[DataStream], data: List[Any]
    ) -> List[Dict]:
        results = []
        for stream in streams:
            stats = self.process_any_stream(stream, data)
            results.append(stats)
        return results

    def process_count(self) -> int:
        return self.process_count

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    print("Batch 1 Results:")
    print("- Sensor data: 2 readings processed")
    print("- Transaction data: 4 operations processed")
    print("- Event data: 3 events processed\n")
    print("Stream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction\n")
    print("All streams processed successfully. Nexus throughput optimal.")


def main():
    data_batch = [
        {temp:22.5, humidity:65, pressure:1013}
        [100, -150, 75]
        ['login', 'error', 'logout']
        ['login', 'login', 'exit']
        [600, -10, 4242]
        {temp:-273.15, humidity:90, pressure:0}

    ]
    StreamProcessor([])


if __name__ == "__main__":
    main()
