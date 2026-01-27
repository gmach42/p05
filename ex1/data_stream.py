from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


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
        """Process a batch of data and return a summary string."""
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data based on given criteria."""
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return statistics about the data stream."""
        return {
            "Stream": self.__class__.__name__,
            "ID": self.stream_id,
            "Type": self.data_type,
            "data_batch": self.processed_data,
            "total_op": len(self.processed_data),
            "analysis": "N/A",
        }

    def get_processed_data(self) -> List[int]:
        return self.processed_data


class SensorStream(DataStream):
    """
    Process environnamental data received as dict{} or list[dict]

    ### Units

    - Temp in °C

    - Humidity in %HR and indicate the percetage of water vapor in a the air
    100 %HR meaning that the air is holding the maximum of water vapor
    by volume unit

    - Pressure in hPa

    - Entropy in J/(kg·K)
    """

    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.data_type = "Environmental Data"
        self.registered_temps: List[float] = []
        self.processed_data: List[Dict] = []

    def process_batch(self, data_batch: List[Dict] | Dict) -> str:
        """Process a batch of environmental data and return a string."""

        # Handle single dict input
        if isinstance(data_batch, dict):
            data_batch = [data_batch]

        for data in data_batch:
            if "temp" in data:
                self.registered_temps.append(data["temp"])
            self.processed_data.append(data)
        display_data = self.format_data(self.processed_data)

        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}\n"
            f"Processing sensor batch: [{display_data}]\n"
            f"Sensor analysis: {len(self.processed_data)} "
            f"readings processed, avg temp: {self.avg_temp():.1f}°C"
        )

    def format_data(self, processed_data: List[int]) -> str:
        """Return formatted display data for transactions."""
        display_data = []
        for data in processed_data:
            if "temp" in data:
                display_data.append(f"temp: {data['temp']}°C")
            if "humidity" in data:
                display_data.append(f"humidity: {data['humidity']}%HR")
            if "pressure" in data:
                display_data.append(f"pressure: {data['pressure']} hPa")
            if "entropy" in data:
                display_data.append(f"entropy: {data['entropy']} J/(kg·K)")
        return ", ".join(display_data)

    def filter_data(
        self,
        data_batch: List[Dict] | Dict,
        criteria: Optional[str] = "critical",
    ) -> List[Dict] | Dict:
        """Filters processed data to return only critical sensor alerts."""

        # Handle single dict input
        if isinstance(data_batch, dict):
            data_batch = [data_batch]

        return [
            data
            for data in data_batch
            if data.get("temp", 0) < 0
            or data.get("temp", 0) > 35
            or data.get("humidity", 0) > 80
            or data.get("pressure", 1013) < 980
            or data.get("pressure", 1013) > 1020
            or data.get("entropy", 0) > 4.0
        ]

    def avg_temp(self) -> float:
        """Calculate the average temperature from registered temperatures."""
        if not self.registered_temps:
            return 0.0
        return sum(self.registered_temps) / len(self.registered_temps)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return statistics about the sensor data stream."""
        res: Dict = super().get_stats()
        nb_analyzed = len(self.processed_data)
        sensor_analysis = {
            "analysis": f"Sensor analysis: {nb_analyzed} readings processed, "
            f"avg temp: {self.avg_temp():.1f}°C"
        }
        return res | sensor_analysis


class TransactionStream(DataStream):
    """Process transactionnal data received as list[int]"""

    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.data_type = "Financial Data"
        self.processed_data: List[int] = []

    def process_batch(self, data_batch: List[int]) -> str:
        """Process a batch of transaction data and return a summary string."""
        for data in data_batch:
            self.processed_data.append(data)
        display_data = self.format_data(self.processed_data)

        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}\n"
            f"Processing transaction batch: [{display_data}]\n"
            f"Transaction analysis: {len(self.processed_data)} "
            f"operations processed, net flow: {self.netflow()} units"
        )

    def format_data(self, processed_data: List[int]) -> str:
        """Return formatted display data for transactions."""
        display_data = []
        for data in processed_data:
            if data >= 0:
                display_data.append(f"buy:{data}")
            else:
                display_data.append(f"sell:{abs(data)}")
        return ", ".join(display_data)

    def filter_data(
        self, data_batch: List[int], criteria: Optional[str] = "large"
    ) -> List[int]:
        """Filter only large transactions."""
        return [d for d in data_batch if abs(d) > 1000]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return statistics about the transaction data stream."""
        res: Dict = super().get_stats()
        transaction_analysis = {
            "analysis": f"Transaction analysis: {len(self.processed_data)} "
            f"operations, net flow: {self.netflow()} units"
        }
        return res | transaction_analysis

    def netflow(self) -> str:
        netflow = sum(self.processed_data)
        if netflow >= 0:
            return f"+{netflow}"
        return str(netflow)


class EventStream(DataStream):
    """Process events' data received as list[str]"""

    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.data_type = "System Events"
        self.errors = []

    def process_batch(self, data_batch: List[str]) -> str:
        """Process a batch of event data and return a summary string."""
        for data in data_batch:
            self.processed_data.append(data)
            if "error" in data.lower():
                self.errors.append(data)
        return (
            f"Stream ID: {self.stream_id}, Type: {self.data_type}\n"
            f"Processing event batch: [{', '.join(self.processed_data)}]\n"
            f"Event analysis: {len(self.processed_data)} events, "
            f"{len(self.errors)} errors detected"
        )

    def format_data(self, processed_data: List[str]) -> str:
        """Return formatted display data for events."""
        return ", ".join(processed_data)

    def filter_data(
        self, data_batch: List[str], criteria: Optional[str] = "error"
    ) -> List[str]:
        """Filter only error events."""
        return [d for d in data_batch if "error" in d.lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return statistics about the event data stream."""
        res = super().get_stats()
        event_analysis = {
            "analysis": f"Event analysis: {len(self.processed_data)} events, "
            f"{len(self.errors)} errors detected"
        }
        return res | event_analysis


class StreamProcessor:
    """
    Dispatch the data batch into the different streams depending on the format
    and process them accordingly.

    1. SensorStream for dict{} data
    2. TransactionStream for list[int] data
    3. EventStream for list[str] data
    """

    def __init__(self):
        self.sensor_streams = []
        self.transaction_streams = []
        self.event_streams = []

    def total_count(self) -> int:
        """Return total number of processed streams."""
        return sum(
            [
                len(self.sensor_streams),
                len(self.transaction_streams),
                len(self.event_streams),
            ]
        )

    def processing_report(self) -> str:
        """Generate a summary report of all processed streams."""
        return (
            f"Total streams processed: {self.total_count()} "
            f"- Sensor data: {len(self.sensor_streams)} readings processed\n"
            f"- Transaction data: "
            f"{len(self.transaction_streams)} transactions processed\n"
            f"- Event data: {len(self.event_streams)} events processed"
        )

    def display_processed_data_by_type(self) -> None:
        """Display processed data grouped by data type."""
        print("\nSensor Data Processed:")
        for stream in self.sensor_streams:
            print(
                f"[{stream.format_data(stream.get_processed_data())}]"
            )
        print("\nTransaction Data Processed:")
        for stream in self.transaction_streams:
            print(
                f"[{stream.format_data(stream.get_processed_data())}]"
            )
        print("\nEvent Data Processed:")
        for stream in self.event_streams:
            print(
                f"[{stream.format_data(stream.get_processed_data())}]"
            )

    def process_any_stream(
        self, data_batch: List[Any], should_display: bool = True
    ) -> DataStream:
        """Process any data batch and return the corresponding DataStream."""
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

        display = stream.process_batch(data_batch)
        if should_display:
            print(display, "\n")
        return stream

    def process_multiple_streams(self, data: List[Any]) -> List[Any]:
        """Process multiple data batches and return combined processed data."""
        sensor_data = []
        transaction_data = []
        event_data = []
        for batch in data:
            self.process_any_stream(batch, should_display=False)
        for stream in self.sensor_streams:
            sensor_data.append(
                stream.format_data(stream.get_processed_data())
            )
        for stream in self.transaction_streams:
            transaction_data.append(
                stream.format_data(stream.get_processed_data())
            )
        for stream in self.event_streams:
            event_data.append(
                stream.format_data(stream.get_processed_data())
            )
        processed_data = sensor_data + transaction_data + event_data
        print(self.processing_report(), "\n")
        return processed_data

    def filtered_data(self) -> Dict[str, List[Any]]:
        """Return filtered data from all streams based on criteria."""
        result = {
            "critical_sensor_data": [],
            "large_transactions": [],
            "error_events": [],
        }
        for stream in self.sensor_streams:
            data = stream.format_data(
                stream.filter_data(stream.get_processed_data())
            )
            if data:
                result["critical_sensor_data"].append(data)
        for stream in self.transaction_streams:
            data = stream.format_data(
                stream.filter_data(stream.get_processed_data())
            )
            if data:
                result["large_transactions"].append(data)
        for stream in self.event_streams:
            data = stream.format_data(
                stream.filter_data(stream.get_processed_data())
            )
            if data:
                result["error_events"].append(data)
        return result

    def filtered_results(self) -> Dict[str, List[Any]]:
        """Display summary of filtered results from all streams."""
        filtered_data = self.filtered_data()
        sensor_alerts = len(filtered_data["critical_sensor_data"])
        large_transactions = len(filtered_data["large_transactions"])
        error_events = len(filtered_data["error_events"])
        print(
            "Filtered results: "
            f"{sensor_alerts} critical sensor alerts, "
            f"{large_transactions} large transactions, "
            f"{error_events} error events"
        )


def main():
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    # Initialize Stream Processor
    stream_processor = StreamProcessor()

    # Testing individual stream processing
    print("Initializing Sensor Stream...")
    sensor_batch = [
        {"temp": 22.5, "humidity": 65, "pressure": 1013},
        {"temp": -5.0},
        {"humidity": 80},
        {"pressure": 1008},
    ]
    stream_processor.process_any_stream(sensor_batch)

    print("Initializing Transaction Stream...")
    transaction_batch = [100, -2500, 75, 5000, -150]
    stream_processor.process_any_stream(transaction_batch)

    print("Initializing Event Stream...")
    event_batch = ["login", "error", "logout"]
    stream_processor.process_any_stream(event_batch)

    print("\n=== Polymorphic Stream Processing ===")
    # Mixed data batch
    data_batch = [
        {"temp": 22.5, "humidity": 65, "pressure": 1013},
        [100, -150, 75],
        ["login", "error", "logout"],
        ["login", "login", "exit"],
        [600, -10, 4242],
        {"temp": -273.15, "humidity": 90, "pressure": 0},
        {"temp": 40.0, "humidity": 30, "pressure": 1000, "entropy": 5},
        ["error: disk full", "warning: high memory usage"],
        [int(10e6), int(-5e6), int(3.5e6), -4520, 1200],
    ]

    # Polymorphic processing
    print("Processing mixed stream types through unified interface...\n")
    processed_data = stream_processor.process_multiple_streams(data_batch)

    print("Raw Processed Data:")
    print(processed_data)
    stream_processor.display_processed_data_by_type()

    # Filtered results
    print("\n=== Filtered datas ===")
    print("\nStream filtering active: High-priority data only.")
    stream_processor.filtered_results()
    print()
    for k in stream_processor.filtered_data():
        print(
            f"{k.capitalize()}:\n{stream_processor.filtered_data()[k]}", "\n"
        )

    print("All streams processed successfully. Nexus throughput optimal.\n")


if __name__ == "__main__":
    main()
