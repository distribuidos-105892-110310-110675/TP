import logging
from typing import Callable


class ReducedData:
    def __init__(self, keys: list[str], accumulator_name: str):
        self._keys = keys
        self._accumulator_name = accumulator_name

        self._reduced_data: dict[tuple, float] = {}

    def reduce_using(
        self,
        batch_item: dict[str, str],
        reduce_function: Callable,
    ) -> None:
        for k in self._keys:
            if batch_item[k] == "":
                logging.warning(f"action: empty_{k} | result: skipped")
                return

        key = tuple(batch_item[k] for k in self._keys)
        if key not in self._reduced_data:
            self._reduced_data[key] = 0

        self._reduced_data[key] = reduce_function(self._reduced_data[key], batch_item)

    def pop_next_batch_item(self) -> dict[str, str]:
        key, value = self._reduced_data.popitem()
        batch_item: dict[str, str] = {}
        for i, k in enumerate(self._keys):
            batch_item[k] = key[i]
        batch_item[self._accumulator_name] = str(value)
        return batch_item

    def is_empty(self) -> bool:
        return len(self._reduced_data) == 0
