
from yt.wrapper import YtClient, yt_dataclass, TypedJob, TablePath
from yt.wrapper.schema import RowIterator
from typing import Iterable
from datetime import datetime
from itertools import groupby


SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/action_stats/output"


@yt_dataclass
class User:
    userid: str
    action: str


@yt_dataclass
class FreqAction:
    action: str 
    freq_action: int
    uniqu: int
    

@yt_dataclass
class Freq:
    action: str
    avg_per_user: float


class Reduce(TypedJob):
    def __call__(self, data: RowIterator[User]) -> Iterable[Freq]:
        action = None
        count_action = 0
        count_uniqu = 0

        for i in groupby(data, key=lambda x: x.userid):
            count_uniqu += 1
            for j in i[1]:
                action = j.action
                count_action += 1
        
        assert action is not None
        
        yield Freq(
            action=action,
            avg_per_user=count_action / count_uniqu
        )

class MapFreq(TypedJob):
    def __call__(self, data: FreqAction) -> Iterable[Freq]:
        yield Freq(
            action=data.action,
            avg_per_user=data.freq_action/data.uniqu
        )


class TrivialMap(TypedJob):
    def __call__(self, data: User) -> Iterable[User]:
        yield data



class Summary_Reduce(TypedJob):
    def __call__(self, data: RowIterator[User]) -> Iterable[User]:
        for i in data:
            yield i
            break
    

def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.run_map(
        TrivialMap(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/users_table"
    )

    client.run_sort(
        source_table="//tmp/users_table",
        sort_by=["action", "userid"]
    )

    client.run_reduce(
        Reduce(),
        source_table="//tmp/users_table",
        destination_table=TARGET_PATH,
        reduce_by=["action"]
    )

    client.run_sort(
        "//tmp/users_table",
        sort_by=["userid"]
    )

    client.run_reduce(
        Summary_Reduce(),
        source_table="//tmp/users_table",
        destination_table="//tmp/uniqu_users",
        reduce_by=["userid"]
    )

    path = TablePath(TARGET_PATH, append=True)

    count_uniqu = client.get_attribute("//tmp/uniqu_users", "row_count")
    count_actions = client.get_attribute("//tmp/users_table", "row_count")

    client.write_table_structured(
        path,
        Freq,
        [Freq(action="total", avg_per_user=count_actions / count_uniqu)]
    )

if __name__ == "__main__":
    main()