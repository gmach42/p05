from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.type = "Generic"

    def get_name(self) -> str:
        return self.stream_id

    def get_type(self) -> str:
        return self.type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.type = "Environmental"

    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class TransactionStream(DataStream):
    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.type = "Financial"

    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self, result: str) -> Dict[str, Union[str, int, float]]:
        pass


class EventStream(DataStream):
    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.type = "System"

    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class StreamProcessor():
    def __init__(self):
        self.process_count = 0

    def process_any_stream(self, stream: DataStream, data: List[Any]) -> str:
        result = stream.process_batch(data)
        stats = stream.get_stats(result)
        print(f"{stream.get_type()} Data: {stats} processed.")
        self.process_count += 1
        return stats

    def process_multiple_streams(
        self, streams: List[DataStream],
        data: List[Any]
    ) -> List[Dict]:
        results = []
        for stream in streams:
            stats = self.process_any_stream(stream, data)
            results.append(stats)
        return results

    def process_count(self) -> int:
        return self.process_count


def main():
    StreamProcessor([])


if __name__ == "__main__":
    main()
