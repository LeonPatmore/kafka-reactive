import time
from collections import OrderedDict
from uuid import uuid4


def uuid() -> str:
    return str(uuid4())


def do_for_n_seconds(do: callable, n: int):
    end_time = time.time() + n
    while time.time() < end_time:
        do()
        time.sleep(1)


def do_until_true_with_timeout(do: callable, timeout_seconds: int = 60):
    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        if do():
            return
        time.sleep(1)
    raise Exception("Timed out!")


def convert_to_ordered_dict(my_dict: dict) -> OrderedDict:
    return OrderedDict(sorted(my_dict.items()))
