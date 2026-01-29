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
    def process(self, data: list[int]) -> str:
        # Processing data
        print(f"Processing data: {data}")
        # Validation
        try:
            if not self.validate(data):
                raise ValueError("Invalid data: All elements must be numeric")
        except ValueError:
            raise
        else:
            print("Validation: Numeric data verified")
        result = []
        for nb in data:
            result.append(nb)
        # Output
        print(f"Output: {self.format_output(result)}\n")
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
    def process(self, data: str) -> str:
        # Processing data
        print(f"Processing data: {data}")
        # Validation
        try:
            if not self.validate(data):
                raise ValueError("Invalid data: Input must be a string")
        except ValueError:
            raise
        else:
            print("Validation: Text data verified")
        result = data
        # Output
        print(f"Output: {self.format_output(result)}\n")
        return result

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        elif "ERROR" in data or "INFO" in data:
            return False
        return True

    def format_output(self, result: str) -> str:
        return (
            f"Processed {len(result)} characters, {len(result.split())} words"
        )


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        # Processing data
        print(f"Processing data: {data}")
        # Validation
        try:
            if not self.validate(data):
                raise ValueError(
                    "Invalid data: Log entry must contain keywords"
                )
        except ValueError:
            raise
        else:
            print("Validation: Log data verified")
        level, _, message = data.partition(": ")
        result = {"level": level, "message": message}
        # Output
        print(f"Output: {self.format_output(result)}\n")
        return result

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        if "ERROR" not in data and "INFO" not in data:
            return False
        return True

    def format_output(self, result: str) -> str:
        if LogProcessor.validate(self, [result]):
            return (
                f"[INFO] {result['level']} level detected: {result['message']}"
            )
        return (
            f"[ALERT] {result['level']} level detected: {result['message']}"
        )


def polymorphic_demo(to_proccess: dict) -> None:
    print("Processing multiple data types through same interface...")
    for i in range(1, 4):
        print(f"Result {i}: ", end="")
        processors = [NumericProcessor, TextProcessor, LogProcessor]
        for p in processors:
            if not p().validate(to_proccess[i - 1]):
                continue
            p().process(to_proccess[i - 1])
            break


def main():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    print("\nInitializing Numeric Processor...")
    numeric_data = [1, 2, 3, 4, 5]
    numeric_processor = NumericProcessor()
    numeric_processor.process(numeric_data)

    print("Initializing Text Processor...")
    text_data = "Hello Nexus World"
    text_processor = TextProcessor()
    text_processor.process(text_data)

    print("Initializing Log Processor...")
    log_data = "ERROR: Connection timeout"
    log_processor = LogProcessor()
    log_processor.process(log_data)

    print("=== Polymorphic Processing Demo ===\n")

    to_process = [[5, 42, -5], "Hello World", "INFO: System ready"]
    polymorphic_demo(to_process)

    print("Foundation systems online. Nexus ready for advanced streams.\n")


if __name__ == "__main__":
    main()
