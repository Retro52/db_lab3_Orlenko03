import json
import config
import psycopg2
from typing import List, Tuple

tables = ["Threads", "LaunchYear", "Lithography", "CPU"]


def get_query_result(_q: str) -> Tuple[list, list]:
    _connection = psycopg2.connect(user=config.username, password=config.password, dbname=config.database,
                                   host=config.host, port=config.port)

    _res = []

    with _connection:
        _cursor = _connection.cursor()
        _cursor.execute(_q)

        for row in _cursor:
            _res.append(row)

    return _res, [_data[0] for _data in _cursor.description]


def export_json(_tables: List[str], _output_file: str = None):
    _output_file = f"db_dump.json" if _output_file is None else _output_file

    _data = {}
    for _table in _tables:
        _rows, _fields = get_query_result(f"SELECT * FROM {_table}")
        _data[_table] = {}
        _data[_table]["fields"] = _fields
        _data[_table]["rows"] = _rows

    with open(_output_file, 'w', newline='', encoding='UTF-8') as _f:
        json.dump(_data, _f, default=str)


def export_tables():
    export_json(tables)
