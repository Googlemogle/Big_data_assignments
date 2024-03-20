from yt.wrapper import YtClient, TypedJob, yt_dataclass
from yt.wrapper.schema import RowIterator
from typing import Iterable
from datetime import datetime, date

# approved bu tests
SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/churned_in_may/output"


@yt_dataclass
class Users:
    userid: str
    timestamp: datetime


@yt_dataclass
class UsersDate:
    userid: str
    date: date


class TrivialMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[Users]:
        yield data


class SimpleReduce(TypedJob):
    def __call__(self, data: RowIterator[Users]) -> Iterable[UsersDate]:
        userid = None
        last_month = None

        for i in data:
            userid = i.userid
            last_month=i.timestamp

        if (last_month.month == 5) and (last_month.year == 2022):
            yield UsersDate(
                userid=userid,
                date=last_month.date()
            )


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.remove(
        "//tmp/churned_in_may",
        force=True
    )
    
    client.run_map(
        TrivialMap(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/churned_in_may"
    )

    client.run_sort(
        "//tmp/churned_in_may", sort_by=["userid", "timestamp"]
    )

    client.run_reduce(
        SimpleReduce(),
        source_table="//tmp/churned_in_may",
        destination_table=TARGET_PATH,
        reduce_by=["userid"]
    )


if __name__ == "__main__":
    main()
