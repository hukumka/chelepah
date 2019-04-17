from dataclasses import dataclass
from typing import Dict
from os import path
import pickle


@dataclass
class Data:
    pair: str
    price: float
    amount: float


class Database:
    UPDATE_SNAP_ONCE_IN = 100

    def __repr__(self):
        return repr(self.data)

    def __init__(self, log_path: str, snapshot_path: str):
        self.log_path = log_path
        self.snapshot_path = snapshot_path
        self.log_updates = 0
        self.data = {}
        self.offset = 0
        self.last_snapshot_offset = 0

        # Get last snapshot
        if path.isfile(snapshot_path):
            snap = self.read_snap()
            self.last_snapshot_offset = snap['offset']
            self.offset = self.last_snapshot_offset
            self.data = snap['data']
            assert self.log_path == snap['log_path']
        # Apply all updates more recent then snapshot
        if path.isfile(log_path):
            for offset, record in self.read_log(self.last_snapshot_offset):
                self.offset = offset
                self.apply_record(record)
                self.log_updates += 1
        else:
            open(log_path, "wb").close()  # create empty file

    def read_snap(self):
        if not path.isfile(self.snapshot_path):
            return {'data': {}, 'offset': 0, 'log_path': self.log_path}
        with open(self.snapshot_path, "rb") as snap:
            try:
                val = pickle.load(snap)
                assert self.log_path == val['log_path']
                return val
            except EOFError:
                return {'data': {}, 'offset': 0, 'log_path': self.log_path}

    def read_log(self, offset=0):
        """
        Read all log records from given offset. Offset must be
        at separation of log records
        """
        with open(self.log_path, "rb") as log:
            log.seek(offset)
            while True:
                try:
                    record = pickle.load(log)
                    yield (log.tell(), record)
                except EOFError:
                    break

    def apply_record(self, record):
        self.data[record['key']] = record['value']

    def get_field(self, id: str):
        return self.data.get(id)

    def update_field(self, key: str, value: Data):
        self.log_updates += 1
        with open(self.log_path, "ab") as log:
            pickle.dump({'value': value, 'key': key}, log)
            self.offset = log.tell()
        self.data[key] = value
        if self.log_updates >= self.UPDATE_SNAP_ONCE_IN:
            with open(self.snapshot_path, "wb") as snap:
                pickle.dump({
                    'data': self.data,
                    'offset': self.offset,
                    'log_path': self.log_path
                }, snap)
        self.log_updates = 0

