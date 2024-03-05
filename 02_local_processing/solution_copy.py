from yt.wrapper import YtClient, yt_dataclass, TablePath
from datetime import datetime, date, timedelta
from itertools import groupby
from typing import Iterable


SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/local_processing/dau"

@yt_dataclass
class Row:
    timestamp: datetime
    userid: str

@yt_dataclass
class Dau:
    date: date
    users: int


def simple_map(data: Iterable) -> Iterable:
    for i in data:
        yield Row(
            timestamp=(i.timestamp - timedelta(hours=3)).date(),
            userid=i.userid
        )

def reduce(data: Iterable, keys: list) -> Iterable:
    def key(x):
        return tuple(getattr(x, i) for i in keys)
    
    sorted_data = sorted(data, key=key)
    grouped_data = groupby(sorted_data, key=key)
    for i in grouped_data:
        for j in i[1]:
            yield j
            break
        

def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    path = TablePath(SOURCE_PATH, columns=["userid", "timestamp"])
    rows = client.read_table_structured(path, Row)
    data = []

    for i in groupby(reduce(simple_map(rows), ["timestamp", "userid"]), lambda x: x.timestamp):
        count = 0
        for j in i[1]:
            count += 1
        data.append(Dau(
            date=i[0],
            users=count
        ))
    

    client.write_table_structured(TARGET_PATH, Dau, data)

            
    


if __name__ == "__main__":
    main()
