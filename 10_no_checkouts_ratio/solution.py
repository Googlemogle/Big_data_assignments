from yt.wrapper import YtClient, yt_dataclass, TypedJob
from yt.wrapper.schema import RowIterator
from itertools import groupby
from typing import Iterable
from datetime import datetime, date


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


class TrivialMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[Users]:
        yield data


class ReduceUsersToDate(TypedJob):
    def __call__(self, data: RowIterator[Users]) -> Iterable[Day]:
        uniqu = 0
        target_users = 0

        for i in groupby(data, key=lambda x: x.userid):
            uniqu += 1
            
            for j in i[1]:
                if j.action == ""


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    ...



if __name__ == "__main__":
    main()
