from yt.wrapper import YtClient, TypedJob, yt_dataclass
from yt.wrapper.schema import RowIterator
from typing import Iterable
from datetime import datetime


SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/checkout_freq/output"


@yt_dataclass
class Users:
    userid: str
    action: str


@yt_dataclass
class Users_freq:
    userid: str
    frequency: int


class SimpleMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[Users]:
        if data.action == "checkout":
            yield data


class SimpleReduce(TypedJob):
    def __call__(self, data: RowIterator[Users]) -> Iterable[Users_freq]:
        count = 0
        user = None
        for i in data:
            count += 1
            user = i.userid
        
        assert user is not None

        yield Users_freq(
            userid=user,
            frequency=count
        )


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.run_map_reduce(
        SimpleMap(),
        SimpleReduce(),
        source_table=SOURCE_PATH,
        destination_table=TARGET_PATH,
        reduce_by=["userid"]
    )

if __name__ == "__main__":
    main()