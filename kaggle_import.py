import sys
import csv
import config
import psycopg2
from typing import List, Tuple, Dict

fname = "amd_processors.csv"
tables_data = {
    "Threads":
        {
            "Fields": ["thread_count"],
            "FieldsCSV": ["threads"],
            "FieldPK": "thread_id",
            "FieldFK": {},
            "FilterCSV": None
        },
    "Lithography":
        {
            "Fields": ["lithography_size"],
            "FieldsCSV": ["lithography"],
            "FieldPK": "lithography_id",
            "FieldFK": {},
            "FilterCSV": None
        },
    "LaunchYear":
        {
            "Fields": ["launchy_year"],
            "FieldsCSV": ["launch_date"],
            "FieldPK": "launchy_id",
            "FieldFK": {},
            "FilterCSV": lambda _x: _x[0:4]
        },
    "CPU":
        {
            "Fields": ["cpu_name"],
            "FieldsCSV": ["name"],
            "FieldPK": "cpu_id",
            "FieldFK": {
                "launchy_id": ["LaunchYear", "launchy_year", "launch_date", lambda _x: _x[0:4]],
                "lithography_id": ["Lithography", "lithography_size", "lithography", None],
                "thread_id": ["Threads", "thread_count", "threads", None]
            },
            "FilterCSV": None
        },
    "DeleteOrder": ["CPU", "LaunchYear", "Lithography", "Threads"]
}


def execute_import():
    _query = import_tables(None, True)

    _connection = psycopg2.connect(user=config.username, password=config.password, dbname=config.database,
                                   host=config.host, port=config.port)

    _res = []

    with _connection:
        _cursor = _connection.cursor()
        try:
            _cursor.execute(_query)
        except Exception as e:
            print("Error occurred while executing query: " + e.__str__())


def import_tables(_t_data: dict = None, _clear_and_fill: bool = True) -> str:
    _tables_data = tables_data if _t_data is None else _t_data
    _fname = fname
    _raw_tables = {}
    _res = ""

    for _table in _tables_data["DeleteOrder"]:
        if _clear_and_fill:
            # removing all data from the table before re-filling it
            _res += f"DELETE FROM {_table};\n"

    for _table in _tables_data:
        if _table == "DeleteOrder":
            break
        _table_data = _tables_data[_table]
        _q, _raw_table = get_populate_request(_fname,
                                              _table,
                                              _table_data["FieldPK"],
                                              _table_data["FilterCSV"],
                                              _table_data["FieldsCSV"],
                                              _table_data["Fields"],
                                              _table_data["FieldFK"],
                                              _raw_tables)
        _raw_tables[_table] = _raw_table
        _res += _q + "\n"
    return _res


def check_data_in_dict(_sample: iter, _src: dict) -> bool:
    for _key, _val in _src.items():
        if _sample == _val:
            return True
    return False


def default_filter(_str):
    return str(_str)


def get_populate_request(_fname: str, _table: str, _pk_field: str, _csv_filter, _params_csv: List[str],
                         _params_table: List[str], _fk_keys: dict = None, _fk_tables: dict = None) -> \
        Tuple[str, Dict[str, dict]]:
    _res = ""
    _ids = 0
    _raw_data = {}
    _csv_filter = default_filter if _csv_filter is None else _csv_filter

    with open(_fname, "r") as _f:
        _reader = csv.DictReader(_f)
        _raw_data[_table] = {}
        _raw_data[_table]["PkKey"] = _pk_field
        for _row in _reader:
            _data = list(filter(lambda _x: _x != "", [str(_csv_filter(_row[_val])) for _val in _params_csv]))
            if not check_data_in_dict({_params_table[_idx]: _data[_idx] for _idx in range(len(_data))},
                                      _raw_data[_table]) and _data != []:

                _fk_fields = [_key for _key, _val in _fk_keys.items()]
                _fk_items = []

                for _fk, _fk_vals in _fk_keys.items():
                    _fk_t_name = _fk_vals[0]
                    _fk_table = _fk_tables[_fk_t_name][_fk_t_name]
                    _local_filter = _fk_vals[3] if _fk_vals[3] is not None else default_filter
                    _sample_val = _local_filter(_row[_fk_vals[2]])
                    for _key, _val in _fk_table.items():
                        if _key == "PkKey":
                            pass
                        else:
                            for _item_key, _item_val in _val.items():
                                if _fk_table["PkKey"] in _fk_fields and str(_item_val) == _sample_val:
                                    _fk_items.append(_key)

                if len(_fk_items) == len(_fk_fields):

                    _new_data = [f"'{_item}'" for _item in _data]
                    _fk_items = [f"'{str(_item)}'" for _item in _fk_items]

                    _res += f"INSERT INTO {_table}({_pk_field}, {', '.join(_params_table)}{', ' if len(_fk_fields) > 0 else ''}{', '.join(_fk_fields)})\n" \
                            f" VALUES({_ids}, {', '.join(_new_data)}{', ' if len(_fk_items) > 0 else ''}{', '.join(_fk_items)});\n"

                    assert len(_params_table) == len(_data)

                    _raw_data[_table][_ids] = {}
                    for _idx in range(len(_params_table)):
                        _raw_data[_table][_ids][_params_table[_idx]] = _data[_idx]

                    _ids += 1
    return _res, _raw_data
