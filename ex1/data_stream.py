from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    """
    Abstract base class for different types of data streams.
    """

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.data_type = "Generic Data"

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        # Process Default implementations
        if criteria != self.data_type:
            return

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        # Process Default implementations
        pass


class SensorStream(DataStream):
    """
    Process environnamental data received as dict{}

    ### Units

    - Temp in °C

    - Humidity in %HR and indicate the percetage of water vapor in a the air
    100 %HR meaning that the air is holding the maximum of water vapor
    by volume unit

    - Pressure in hPa

    - Entropy in J/(kg·K)
    """

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "Environmental Data"
        self.nb_reading = 0
        self.temps = []

    def process_batch(self, data_batch: List[Any]) -> str:
        new_temps = [d["temp"] for d in data_batch if "temp" in d]
        self.temps.append(new_temps)

        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}"
            f"Processing event batch: {data_batch}"
            f"Sensor analysis: {self.nb_reading} readings processed, "
            f"avg temp: {self.avg_temp()}°C"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        # filter only critical sensor alerts
        if criteria == "critical":
            return [
                d
                for d in data_batch
                if d.get("temp", 0) < 0 or d.get("temp", 0) > 35
            ]
        return super().filter_data(data_batch, criteria)

    def avg_temp(self) -> float:
        return sum(enumerate(self.temps)) / len(self.temps)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Sensor analysis": f"{self.nb_reading} readings processed, avg temp: {self.avg_temp()}°C"
        }


class TransactionStream(DataStream):
    """Process transactionnal data received as list[int]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "Financial Data"
        self.operations_count = 0
        self.netflow = 0

    def process_batch(self, data_batch: List[int]) -> str:
        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}"
            f"Processing event batch: {data_batch}"
            f"Transaction analysis: {self.operations_count} operations, "
            f"net flow: {self.netflow} units"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        # filter only large transactions
        if criteria == "large":
            return [d for d in data_batch if abs(d) > 1000]
        return super().filter_data(data_batch, criteria)

    def get_stats(self, result: str) -> Dict[str, Union[str, int, float]]:
        pass


class EventStream(DataStream):
    """Process events' data received as list[str]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "System Events"
        self.events_count = 0
        self.errors = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}"
            f"Processing event batch: {data_batch}"
            f"Event analysis: {self.events_count} events, "
            f"{self.errors} error detected"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        # filter only error events
        if criteria == "error":
            return [d for d in data_batch if "error" in d.lower()]
        return super().filter_data(data_batch, criteria)

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


def main():
    data_batch = [
        {"temp": 22.5, "humidity": 65, "pressure": 1013},
        [100, -150, 75],
        ["login", "error", "logout"],
        ["login", "login", "exit"],
        [600, -10, 4242],
        {"temp": -273.15, "humidity": 90, "pressure": 0},
        {"temp": 40.0, "humidity": 30, "pressure": 1000, "entropy": 5},
        ["error: disk full", "warning: high memory usage"],
        [10e6, -5e6, 3.5e6, -4520, 1200]
    ]
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    stream_processor = StreamProcessor()

    print("Initializing Sensor Stream...")
    stream_processor.process_any_stream(SensorStream("SENSOR-001"), data_batch)

    print("\nInitializing Transaction Stream...")
    stream_processor.process_any_stream(
        TransactionStream("TRANS-001"), data_batch
    )

    print("\nInitializing Event Stream...")
    stream_processor.process_any_stream(EventStream("EVENT-001"), data_batch)

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    stream_processor.process_multiple_streams(
        [
            SensorStream("SENSOR-002"),
            TransactionStream("TRANS-002"),
            EventStream("EVENT-002"),
        ],
        data_batch,
    )

    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
