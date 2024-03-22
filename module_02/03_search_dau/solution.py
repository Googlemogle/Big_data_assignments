from yt.wrapper import YtClient, TablePath, TypedJob, yt_dataclass
from datetime import datetime, date
from typing import Iterable
from yt.wrapper.schema import RowIterator


SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/search_dau/output"


@yt_dataclass
class Users:
    userid: str
    action: str
    timestamp: datetime


@yt_dataclass
class UserDate:
    userid: str
    timestamp: date


@yt_dataclass
class DAU:
    date: date
    users: int


class SimpleMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[UserDate]:
        if data.action == "search":
            yield UserDate(
                userid=data.userid,
                timestamp=data.timestamp.date()
            )


class SimpleReduce(TypedJob):
    def __call__(self, data: RowIterator[UserDate]) -> Iterable[UserDate]:
        for i in data:
            yield i
            break
        

class TrivialMapUsers(TypedJob):
    def __call__(self, data: UserDate) -> Iterable[UserDate]:
        yield data


class ReduceToDAU(TypedJob):
    def __call__(self, data: UserDate) -> Iterable[DAU]:
        count = 0
        dt = None
        for i in data:
            count += 1
            dt = i.timestamp
        yield DAU(
            date=dt,
            users=count
        )


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.run_map_reduce(
        SimpleMap(),
        SimpleReduce(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/Search_DAU_1",
        reduce_by=["timestamp", "userid"]
    )

    client.run_map_reduce(
        TrivialMapUsers(),
        ReduceToDAU(),
        source_table="//tmp/Search_DAU_1",
        destination_table=TARGET_PATH,
        reduce_by=["timestamp"]
    )

if __name__ == "__main__":
    main()