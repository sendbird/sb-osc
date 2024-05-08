import re
from datetime import datetime


def from_string(value: str):
    if value.isdigit():
        return int(value)
    elif value.isdecimal() or value.replace('.', '', 1).isdigit():
        return float(value)
    elif re.match(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', value):
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    elif value == 'True':
        return True
    elif value == 'False':
        return False
    else:
        return value


def to_string(value):
    if isinstance(value, datetime):
        return value.isoformat(' ', 'seconds')
    else:
        return str(value)
