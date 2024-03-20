from yt.wrapper import YtClient, yt_dataclass, TypedJob, aggregator
from yt.wrapper.schema import RowIterator
from datetime import datetime
from typing import Iterable


SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/max_acquisition_month/output"


@yt_dataclass
class Users:
    userid: str
    timestamp: datetime


@yt_dataclass
class UserMonth:
    userid: str
    month: str


@yt_dataclass
class Month:
    month: str
    count: int


class SimpleMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[Users]:
        yield data


class SimpleReduce_to_start_using(TypedJob):
    def __call__(self, data: RowIterator[Users]) -> Iterable[UserMonth]:
        for i in data:
            yield UserMonth(
                userid = i.userid,
                month = i.timestamp.strftime("%Y%m")
            )
            break


class ReduceToMonths(TypedJob):
    def __call__(self, data: RowIterator[UserMonth]) -> Iterable[Month]:
        month = None
        count = 0
        
        for i in data:
            month = i.month
            count += 1

        assert month is not None
        
        yield Month(
            month = month,
            count = count
        )


@aggregator
class MapMaxofMonth(TypedJob):
    def __call__(self, data: Iterable[Month]) -> Iterable[Month]:
        max = None

        for i in data:
            max = i

        yield max
    

def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    

    client.run_map(
        SimpleMap(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/users_month"
    )

    client.run_sort(
        "//tmp/users_month",
        sort_by=["userid", "timestamp"]
    )

    client.run_reduce(
        SimpleReduce_to_start_using(),
        source_table="//tmp/users_month",
        destination_table="//tmp/user_month_get_start",
        reduce_by=["userid"]
    )

    client.run_sort(
        "//tmp/user_month_get_start",
        sort_by=["month"]
    )

    client.run_reduce(
        ReduceToMonths(),
        source_table="//tmp/user_month_get_start",
        destination_table="//tmp/months_freq",
        reduce_by=["month"]
    )

    client.run_sort(
        "//tmp/months_freq",
        sort_by=["count", "month"]
    )

    client.run_map(
        MapMaxofMonth(),
        source_table="//tmp/months_freq",
        destination_table=TARGET_PATH
    )


if __name__ == "__main__":
    main()
