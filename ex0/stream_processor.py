from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"{result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        result = []
        for nb in data:
            result.append(nb)
        return result

    def validate(self, data: Any) -> bool:
        for nb in data:
            if not isinstance(nb, (int, float)):
                return False
        return True

    def avg(self, data: list) -> float:
        if not data:
            raise ValueError("Cannot compute average of empty list")
        return sum(data) / len(data)

    def format_output(self, result: str) -> str:
        return (
            f"Processed {len(result)} numeric values, "
            f"sum={sum(result)}, avg={self.avg(result)}"
        )


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        result = data
        return result

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        return True

    def format_output(self, result: str) -> str:
        return (
            f"Processed {len(result)} characters, {len(result.split())} words"
        )


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        level, _, message = data.partition(": ")
        result = {
            "level": level,
            "message": message
        }
        return result

    def validate(self, data: Any) -> bool:
        for entry in data:
            if "ERROR" not in entry:
                return False
        return True

    def format_output(self, result: str) -> str:
        if LogProcessor.validate(self, [result]):
            return (
                f"[INFO] {result['level']} level detected: "
                f"{result['message']}")
        return (
            f"[ALERT] {result['level']} level detected:\n"
            f"{result['message']}"
        )


def polymorphic_demo(to_proccess: dict):
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types with a single interface.")
    for i in range(1, 3):
        print(f"\n--- Iteration {i} ---")
        processor, data = to_proccess[list(to_proccess.keys())[i - 1]]
        print(f"Result {i}: {processor.format_output(processor.process(data[i - 1]))}")


def main():
    numeric_data = [10, 20, 30, 40, 50]
    text_data = "Hello world! This is a test."
    log_data = "ERROR: Disk space low"

    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    to_process = {
        "Numeric Data": (numeric_processor, numeric_data),
        "Text Data": (text_processor, text_data),
        "Log Data": (log_processor, log_data)
    }
    polymorphic_demo(to_process)


if __name__ == "__main__":
    main()
