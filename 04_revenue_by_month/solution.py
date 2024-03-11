from yt.wrapper import YtClient, yt_dataclass, TypedJob
from yt.wrapper.schema import RowIterator
from datetime import datetime, date
from typing import Iterable  

SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/revenue_by_month/output"


@yt_dataclass
class Users:
    action: str
    timestamp: datetime
    value: float


@yt_dataclass
class UsersDate:
    month: str
    value: float


class SimpleMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[UsersDate]:
        if data.action == "confirmation":
            yield UsersDate(
                month=data.timestamp.strftime("%Y-%m"),
                value=data.value
            )


class SimpleReduce(TypedJob):
    def __call__(self, data: RowIterator[UsersDate]) -> Iterable[UsersDate]:
        sum = 0
        month = None
        for i in data:
            sum += i.value
            month = i.month
        
        assert month is not None

        yield UsersDate(
            month=month,
            value=sum
        )


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.run_map_reduce(
        SimpleMap(),
        SimpleReduce(),
        source_table=SOURCE_PATH,
        destination_table=TARGET_PATH,
        reduce_by=["month"]
    )



if __name__ == "__main__":
    main()