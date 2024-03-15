
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
    def __call__(self, data: RowIterator[User]) -> Iterable[FreqAction]:
        action = None
        count_action = 0
        count_uniqu = 0

        for i in groupby(data, key=lambda x: x.userid):
            count_uniqu += 1
            for j in i[1]:
                action = j.action
                count_action += 1
        
        assert action is not None
        
        yield FreqAction(
            action=action,
            freq_action=count_action,
            uniqu=count_uniqu
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
    uniqu = 0
    count = 0

    def __call__(self, data: RowIterator[User]) -> Iterable[Freq]:
        uniqu += 1

        for i in data:
            count += 1

    def __del__(self) -> Iterable[Freq]:
        count = count
        uniqu = uniqu

        return Freq(
            action="total",
            avg_per_user=count/uniqu
        )
    

def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    

    client.run_sort(
        source_table=SOURCE_PATH,
        sort_by=["action", "userid"]
    )

    client.run_reduce(
        Reduce(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/action_stats",
        reduce_by=["action"]
    )

    client.run_map(
        MapFreq(),
        source_table="//tmp/action_stats",
        destination_table=TARGET_PATH,
    )

    client.run_map_reduce(
        TrivialMap(),
        Summary_Reduce(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/stats_each_actions",
        reduce_by=["userid"]
    )

    client.run_merdge(

    )

if __name__ == "__main__":
    main()