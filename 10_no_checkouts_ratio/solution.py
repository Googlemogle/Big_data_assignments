from yt.wrapper import YtClient, yt_dataclass, TypedJob
from yt.wrapper.schema import RowIterator
from itertools import groupby
from typing import Iterable
from datetime import datetime, date

# approved by tests
SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/no_checkouts_ratio/output"


@yt_dataclass
class Day:
    date: date
    users_share: float


@yt_dataclass
class Users:
    userid: str
    timestamp: datetime
    action: str


@yt_dataclass
class UsersDate:
    userid: str
    date: date
    action: str


class TrivialMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[UsersDate]:
        yield UsersDate(
            userid = data.userid,
            date = data.timestamp.date(),
            action = data.action
        )


class ReduceUsersToDate(TypedJob):
    def __call__(self, data: RowIterator[UsersDate]) -> Iterable[Day]:
        date = None
        uniqu = 0
        target_users = 0

        for i in groupby(data, key=lambda x: x.userid):
            uniqu += 1
            is_prod = False
            is_confirmation = False
            for j in i[1]:
                date = j.date
                if j.action == "product":
                    is_prod = True
                if j.action == "checkout":
                    is_confirmation = True
            if is_prod and not is_confirmation:
                target_users += 1
        
        assert uniqu != 0
        assert date is not None

        yield Day(
            date = date,
            users_share = target_users / uniqu
        )


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.remove(
        "//tmp/users_no_checkouts",
        force=True
    )

    client.run_map(
        TrivialMap(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/users_no_checkouts",
    )

    client.run_sort(
        "//tmp/users_no_checkouts",
        sort_by = ["date", "userid"]
    )

    client.run_reduce(
        ReduceUsersToDate(),
        source_table="//tmp/users_no_checkouts",
        destination_table=TARGET_PATH,
        reduce_by=["date"]
    )



if __name__ == "__main__":
    main()
