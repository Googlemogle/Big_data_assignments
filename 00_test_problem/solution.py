from yt.wrapper import YtClient


SOURCE_PATH = "//home/student"


def main():
    
    client = YtClient(proxy="127.0.0.1:8000", config={"proxy": {"enable_proxy_discovery": False}})
    # 1. Подключитесь к локальному кластеру YTsaurus по адресу 127.0.0.1:8000 с помощью Python API
    print(*sorted(client.list(SOURCE_PATH)), sep="\n")
    # 2. С помощью команды list получите список узлов, расположенных в директории SOURCE_PATH
    
    # 3. Выведите названия полученных узлов в лексикографическом порядке, по одному на каждой строке
    


if __name__ == "__main__":
    main()
