from typing import Any, List, Dict, Union, Optional, Generator
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
        # Filter default implementations
        if criteria != self.data_type:
            print(f"Wrong data_format send for {self.data_type}")
            return

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        # Stats default implementations
        # Initializing Sensor Stream...
        # Stream ID: SENSOR_001, Type: Environmental Data
        # Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]
        # Sensor analysis: 3 readings processed, avg temp: 22.5°C
        return {
            "Stream": self.__class__.__name__,
            "ID": self.stream_id,
            "Type": self.data_type,
            "data_batch": self.processed_batch,
            "total_op": self.total_op,
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
        self.nb_reading = 0
        self.temps = []

    def process_batch(self, data_batch: List[Dict] | Dict) -> str:
        new_temps = [d["temp"] for d in data_batch if "temp" in d]
        self.temps.append(new_temps)

        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}"
            f"Processing event batch: {data_batch}"
            f"Sensor analysis: {self.nb_reading} readings processed, "
            f"avg temp: {self.avg_temp()}°C"
        )

    def filter_data(
        self,
        data_batch: List[Dict] | Dict,
        criteria: Optional[str] = "critical",
    ) -> List[Dict] | Dict:
        # filter only critical sensor alerts
        super().filter_data(data_batch, criteria)
        return [
            d
            for d in data_batch
            if d.get("temp", 0) < 0 or d.get("temp", 0) > 35
        ]

    def avg_temp(self) -> float:
        return sum(enumerate(self.temps)) / len(self.temps)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Sensor analysis": f"{self.nb_reading} readings processed, "
            "avg temp: {self.avg_temp()}°C"
        }


class TransactionStream(DataStream):
    """Process transactionnal data received as list[int]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "Financial Data"
        self.operations_count = 0
        self.netflow = 0

    def process_batch(self, data_batch: List[int]) -> str:
        data_batch_sum = sum(data_batch)
        self.operations_count += len(data_batch)
        self.netflow += data_batch_sum
        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}"
            f"Processing event batch: {data_batch}"
            f"Transaction analysis: {self.operations_count} operations, "
            f"net flow: {self.netflow} units"
        )

    def filter_data(
        self, data_batch: List[int], criteria: Optional[str] = "large"
    ) -> List[int]:
        # filter only large transactions
        return [d for d in data_batch if abs(d) > 1000]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class EventStream(DataStream):
    """Process events' data received as list[str]"""

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.data_type = "System Events"
        self.events_count = 0
        self.errors = 0

    def process_batch(self, data_batch: List[str]) -> Generator:
        # TODO create a generator of the usefull data?
        pass

    def filter_data(
        self, data_batch: List[str], criteria: Optional[str] = "error"
    ) -> List[str]:
        # filter only error events
        return [d for d in data_batch if "error" in d.lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class StreamProcessor:
    """
    Dispatch the data batch into the different streams depending on the format
    """

    def __init__(self):
        self.sensor_processed = 0
        self.transaction_processed = 0
        self.event_processed = 0

    def process_multiple_streams(self, data: List[Any]):
        for batch in data:
            self.process_any_stream(batch)
        print(self.processing_report(), "\n")

    def total_count(self) -> int:
        return sum(
            [
                self.sensor_processed,
                self.transaction_processed,
                self.event_processed,
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
        if isinstance(data_batch, Dict | List[Dict]):
            self.sensor_processed += 1
            stream_id = f"SENSOR-{self.sensor_processed}"
            SensorStream(stream_id).process_batch(data_batch)

        elif isinstance(data_batch, List[int]):
            self.transaction_processed += 1
            stream_id = f"TRANS-{self.transaction_processed}"
            TransactionStream(stream_id).process_batch(data_batch)

        elif isinstance(data_batch, List[str]):
            self.event_processed += 1
            stream_id = f"EVENT-{self.event_processed}"
            EventStream(stream_id).process_batch(data_batch)

        else:
            print(f"Unknown data format: {data_batch}")

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
