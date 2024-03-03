from yt.wrapper import YtClient
from dataclasses import dataclass
from typing import Iterable


SOURCE_PATH = "//home/student/examine"

# @dataclass
# class Table:
#     path: str
#     chunks: int
#     size: int

# def parse_tables(client, data) -> Iterable:
#     for name in client.list(data, attributes=["chunk_count", "compressed_data_size"]):
#         yield Table(
#             path=str(name),
#             chunks=name.attributes.get("chunk_count"),
#             size=name.attributes.get("compressed_data_size")
#         )


def main():
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    
    for i in sorted(
            filter(lambda x: len(x.attributes.values()) == 2, client.list(SOURCE_PATH, attributes=["compressed_data_size", "chunk_count"])), 
            key=lambda x: (-x.attributes["chunk_count"], -x.attributes["compressed_data_size"])
            ):
        print(i, i.attributes["chunk_count"], i.attributes["compressed_data_size"])

    


if __name__ == "__main__":
    main()
