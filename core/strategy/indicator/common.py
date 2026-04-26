import datetime
import pandas as pd

class SignalRecordManager:
    def __init__(self):
        self.signal_records = []

    def add_signal_record(self, date, signal_type, signal_description):
        self.signal_records.append(SignalRecord(date, signal_type, signal_description))

    def transform_to_dataframe(self):
        return pd.DataFrame([record.__dict__ for record in self.signal_records])

class SignalRecord:
    def __init__(self, date, signal_type, signal_description):
        if type(date) is datetime.date:
            # 将datetime.date转换为pandas Timestamp
            self.date = pd.Timestamp(date)
            # self.date = date.strftime('%Y-%m-%d')
        elif type(date) is str:
            # 将字符串转换为pandas Timestamp
            self.date = pd.Timestamp(date)
            # self.date = date
        else:
            raise ValueError('date must be datetime.date or str')
        self.signal_type = signal_type
        self.signal_description = signal_description