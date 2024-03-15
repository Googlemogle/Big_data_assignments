
from yt.wrapper import YtClient, yt_dataclass, TypedJob, TablePath
from yt.wrapper.schema import RowIterator
from typing import Iterable


SOURCE_PATH = "//home/student/logs/user_activity_log"


@yt_dataclass
class Users:
    userid: str
    action: str


@yt_dataclass
class UsersData:
    userid: str
    count: int


class SimpleMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[Users]:
        if data.action == "checkout":
            yield data


class SimpleReduce(TypedJob):
    def __call__(self, data: RowIterator[Users]) -> Iterable[UsersData]:
        count = 0
        user = None
        for i in data:
            count += 1
            user = i.userid

        assert user is not None

        yield UsersData(
            userid=user,
            count=-count
        )


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.remove(
        "//tmp/top_buyers"
    )

    client.run_map_reduce(
        SimpleMap(),
        SimpleReduce(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/top_buyers",
        reduce_by=["userid"]
    )

    
    client.run_sort(
        source_table="//tmp/top_buyers",
        sort_by=["count", "userid"]
    )

    # count_rows = client.get("//tmp/top_buyers/@row_count")
    path = TablePath("//tmp/top_buyers", start_index=0, end_index=10)
    top = []

    for i in client.read_table_structured(path, UsersData):
        top.append(i)
        
    for i in top:
        print(i.userid, -i.count)

if __name__ == "__main__":
    main()
