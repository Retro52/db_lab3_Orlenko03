import csv
import config
import psycopg2


tables = ["Threads", "LaunchYear", "Lithography", "CPU"]


def export_csv(_table_name: str, _output_file: str = None):
    _connection = psycopg2.connect(user=config.username, password=config.password, dbname=config.database,
                                   host=config.host, port=config.port)

    _cur = _connection.cursor()

    _output_file = f"{_table_name.lower()}.csv" if _output_file is None else _output_file

    with open(_output_file, "w", newline='', encoding='utf-8') as _f:
        _cur.execute(f"SELECT * FROM {_table_name}")
        _writer = csv.writer(_f)
        _writer.writerow([_item[0] for _item in _cur.description])

        for _row in _cur:
            _writer.writerow([str(_item) for _item in _row])


def export_tables():
    for _table in tables:
        export_csv(_table)
