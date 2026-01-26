from typing import Any, List, Dict, Union, Optional, Generator
from abc import ABC, abstractmethod
from unittest import result


class DataStream(ABC):
    """
    Abstract base class for different types of data streams.
    """

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.data_type = "Generic Data"
        self.processed_data: List[Any] = []

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        # Filter default implementations
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Stream": self.__class__.__name__,
            "ID": self.stream_id,
            "Type": self.data_type,
            "data_batch": self.processed_data,
            "total_op": len(self.processed_data),
            "analysis": "N/A",
        }


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
        self.registered_temps: List[float] = []

    def process_batch(self, data_batch: List[Dict] | Dict) -> str:
        # Handle single dict input
        if isinstance(data_batch, dict):
            data_batch = [data_batch]

        for data in data_batch:
            if "temp" in data:
                self.registered_temps.append(data["temp"])
                self.processed_data.append(data["temp"])
            if "humidity" in data:
                self.processed_data.append(data["humidity"])
            if "pressure" in data:
                self.processed_data.append(data["pressure"])
            if "entropy" in data:
                self.processed_data.append(data["entropy"])
        return (
            f"Sensor analysis: {len(self.processed_data)} "
            f"readings processed, avg temp: {self.avg_temp():.1f}°C"
        )

    def filter_data(
        self,
        data_batch: List[Dict] | Dict,
        criteria: Optional[str] = "critical",
    ) -> List[Dict] | Dict:
        # Returns only critical sensor alerts
        return [
            d
            for d in data_batch
            if d.get("temp", 0) < 0
            or d.get("temp", 0) > 35
            or d.get("humidity", 100) > 80
            or d.get("pressure", 0) < 980
            or d.get("pressure", 0) > 1020
            or d.get("entropy", 0) > 4.0
        ]

    def avg_temp(self) -> float:
        if not self.registered_temps:
            return 0.0
        return sum(self.registered_temps) / len(self.registered_temps)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        res: Dict = super().get_stats()
        sensor_analysis = {
            "analysis": f"Sensor analysis: {len(self.processed_data)} readings processed, "
            f"avg temp: {self.avg_temp():.1f}°C"
        }
        return res | sensor_analysis


class TransactionStream(DataStream):
    """Process transactionnal data received as list[int]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "Financial Data"

    def process_batch(self, data_batch: List[int]) -> str:
        for data in data_batch:
            self.processed_data.append(data)
        return (
            f"Transaction analysis: {len(self.processed_data)} "
            f"operations processed, net flow: {self.netflow()}"
        )

    def filter_data(
        self, data_batch: List[int], criteria: Optional[str] = "large"
    ) -> List[int]:
        # filter only large transactions
        return [d for d in data_batch if abs(d) > 1000]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        res: Dict = super().get_stats()
        transaction_analysis = {
            "analysis": f"Transaction analysis: {len(self.processed_data)} "
            f"operations, net flow: {self.netflow()} units"
        }
        return res | transaction_analysis

    def netflow(self) -> int:
        return sum(self.processed_data)


class EventStream(DataStream):
    """Process events' data received as list[str]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "System Events"
        self.errors = []

    def process_batch(self, data_batch: List[str]) -> str:
        for data in data_batch:
            self.processed_data.append(data)
            if "error" in data.lower():
                self.errors.append(data)
        return (
            f"Event analysis: {len(self.processed_data)} events, "
            f"{len(self.errors)} errors detected"
        )

    def filter_data(
        self, data_batch: List[str], criteria: Optional[str] = "error"
    ) -> List[str]:
        # filter only error events
        return [d for d in data_batch if "error" in d.lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        res = super().get_stats()
        event_analysis = {
            "analysis": f"Event analysis: {len(self.processed_data)} events, "
            f"{len(self.errors)} errors detected"
        }
        return res | event_analysis


class StreamProcessor:
    """
    Dispatch the data batch into the different streams depending on the format
    """

    def __init__(self):
        self.sensor_streams = []
        self.transaction_streams = []
        self.event_streams = []

    def process_multiple_streams(self, data: List[Any]):
        for batch in data:
            self.process_any_stream(batch)
        print(self.processing_report(), "\n")

    def total_count(self) -> int:
        return sum(
            [
                len(self.sensor_processed),
                len(self.transaction_processed),
                len(self.event_processed),
            ]
        )

    def processing_report(self) -> str:
        return (
            f"Total streams processed: {self.total_count()} "
            f"- Sensor data: {self.sensor_processed} readings processed\n"
            f"- Transaction data: "
            f"{self.transaction_processed} transactions processed\n"
            f"- Event data: {self.event_processed} events processed"
        )

    def process_any_stream(self, data_batch: List[Any]) -> None:
        if isinstance(data_batch, dict) or (
            isinstance(data_batch, list)
            and all(isinstance(d, dict) for d in data_batch)
        ):
            stream_id = f"SENSOR-{len(self.sensor_streams) + 1:03d}"
            stream = SensorStream(stream_id)
            self.sensor_streams.append(stream)

        elif isinstance(data_batch, list) and all(
            isinstance(d, int) for d in data_batch
        ):
            stream_id = f"TRANS-{len(self.transaction_streams) + 1:03d}"
            stream = TransactionStream(stream_id)
            self.transaction_streams.append(stream)

        elif isinstance(data_batch, list) and all(
            isinstance(d, str) for d in data_batch
        ):
            stream_id = f"EVENT-{len(self.event_streams) + 1:03d}"
            stream = EventStream(stream_id)
            self.event_streams.append(stream)

        else:
            print(f"Unknown data format: {data_batch}")
            return
        # Process the batch with the appropriate stream (polymorphism)
        stream.process_batch(data_batch)

    def display_single_stream_stats(self, stream: DataStream) -> None:
        stats = stream.get_stats()
        print(
            f"Stream ID: {stats['ID']}, Type: {stats['Type']}\n"
            f"Total data points: {stats['total_op']}\n"
            f"Analysis: {stats['analysis']}\n"
        )

    def filtered_results(
        self, streams: DataStream, processed_data
    ) -> Dict[str, List[Any]]:
        filtered_sensor_data = SensorStream.filter_data(
            self, processed_data, criteria="critical"
        )
        filtered_transaction_data = TransactionStream.filter_data(
            self, processed_data, criteria="large"
        )
        filtered_event_data = EventStream.filter_data(
            self, processed_data, criteria="error"
        )
        return {
            "critical_sensor_data": filtered_sensor_data,
            "large_transactions": filtered_transaction_data,
            "error_events": filtered_event_data,
        }


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
        [10e6, -5e6, 3.5e6, -4520, 1200],
    ]
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    stream_processor = StreamProcessor()

    print("Initializing Sensor Stream...")
    sensor_batch = [
        {"temp": 22.5, "humidity": 65, "pressure": 1013},
        {"temp": -5.0},
        {"humidity": 80},
        {"pressure": 1008},
    ]
    stream_processor.process_any_stream(sensor_batch)

    print("\nInitializing Transaction Stream...")
    transaction_batch = [100, -2500, 75, 5000, -150]
    stream_processor.process_any_stream(transaction_batch)

    print("\nInitializing Event Stream...")
    event_batch = ["login", "error", "logout"]
    stream_processor.process_any_stream(event_batch)

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    stream_processor.process_multiple_streams(data_batch)

    print("Stream filtering active: High-priority data only.")
    print(f"Filtered results: {stream_processor.filtered_results}\n")

    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
