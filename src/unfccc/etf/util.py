from collections.abc import Iterator
import itertools
import logging
import sys

try:
    pairwise = itertools.pairwise
except ImportError:
    def pairwise(items):
        if not isinstance(items, Iterator):
            items = iter(items)
        items0, items1 = itertools.tee(items)
        next(items1, None)
        return zip(items0, items1)


def pformat_size(size):
    return (
        f'{size // 1048576}M' if size >= 1024 * 1024
        else (f'{size // 1024}k' if size >= 1024 else size)
    )


def sizeof_dict(obj):
    result = sys.getsizeof(obj)
    for (key, value) in obj.items():
        result += sys.getsizeof(key)
        if isinstance(value, (str, int, float)):
            result += sys.getsizeof(value)
    return result


class BiFormatter(logging.Formatter):

    debug_format = '%(relativeCreated)d\t%(levelname)s\t%(message)s'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        kwargs.pop('fmt', None)
        self.debug_formatter = logging.Formatter(fmt=self.debug_format,
                                                 **kwargs)

    def format(self, record):
        if record.levelno < logging.INFO:
            return self.debug_formatter.format(record)
        return super().format(record)
