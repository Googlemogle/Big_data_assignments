from yt.wrapper import YtClient, yt_dataclass, TypedJob, reduce_aggregator
from yt.wrapper.schema import RowIterator
from typing import Iterable
from datetime import datetime, date, timedelta

# approved by tests
SOURCE_PATH = "//home/student/logs/user_activity_log"

TARGET_PATH = "//home/student/assignments/user_sessions/output"


@yt_dataclass
class Users:
    userid: str
    action: str
    timestamp: datetime
    value: float
    testids: str


@yt_dataclass
class UsersSessions(Users):
    sessionid: str


class TrivialMap(TypedJob):
    def __call__(self, data: Users) -> Iterable[Users]:
        yield data


@reduce_aggregator
class ReduceSessions(TypedJob):
    def __call__(self, data: Iterable[RowIterator[Users]]) -> Iterable[UsersSessions]:
        session = 0

        for j in data:
            session += 1
            first = next(j)

            yield UsersSessions(
                    userid=first.userid,
                    timestamp=first.timestamp,
                    action=first.action,
                    value=first.value,
                    testids=first.testids,
                    sessionid=str(session)
                )
            
            for i in j:
                
                if i.timestamp - first.timestamp > timedelta(minutes=30):
                    session += 1
                
                first = i

                yield UsersSessions(
                        userid=i.userid,
                        action=i.action,
                        timestamp=i.timestamp,
                        value=i.value,
                        testids=i.testids,
                        sessionid=str(session)
                    )


def main():
    client = YtClient(proxy="127.0.01:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    client.remove(
        "//tmp/users_sessions",
        force=True
    )

    client.run_map(
        TrivialMap(),
        source_table=SOURCE_PATH,
        destination_table="//tmp/users_sessions"
    )

    client.run_sort(
        "//tmp/users_sessions",
        sort_by=["userid", "timestamp"]
    )
    
    client.run_reduce(
        ReduceSessions(),
        source_table="//tmp/users_sessions",
        destination_table=TARGET_PATH,
        reduce_by=["userid"]
    )

    # client.run_sort(
    #     TARGET_PATH,
    #     sort_by=["userid", "timestamp"]
    # )



if __name__ == "__main__":
    main()
