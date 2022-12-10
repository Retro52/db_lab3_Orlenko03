import matplotlib.pyplot as plt
from typing import List, Tuple
import matplotlib
import psycopg2
import config
import sys


matplotlib.use('TkAgg')

view_1_name = "CPUsReleasedByYear"
query_1 = f'''
CREATE VIEW {view_1_name} AS
SELECT launchyear.launchy_year, COUNT(cpu_name) FROM cpu INNER JOIN launchyear
ON launchyear.launchy_id = cpu.launchy_id
GROUP BY launchyear.launchy_year
ORDER BY launchyear.launchy_year DESC
'''

view_2_name = "ThreadsCountDistribution"
query_2 = f'''
CREATE VIEW {view_2_name} AS
SELECT threads.thread_count, COUNT(cpu_name) FROM cpu INNER JOIN threads
ON threads.thread_id = cpu.thread_id
GROUP BY threads.thread_count
ORDER BY COUNT(cpu_name), threads.thread_count
'''

view_3_name = "MinimalLithographyByYear"
query_3 = f'''
CREATE VIEW {view_3_name} AS
SELECT launchyear.launchy_year, MIN(lithography.lithography_size)
FROM cpu
JOIN launchyear
ON launchyear.launchy_id = cpu.launchy_id
JOIN lithography
ON lithography.lithography_id = cpu.lithography_id
GROUP BY launchyear.launchy_year
ORDER BY launchyear.launchy_year
'''


def get_query_result(_q: List[str]) -> Tuple[list, list]:
    _connection = psycopg2.connect(user=config.username, password=config.password, dbname=config.database,
                                   host=config.host, port=config.port)

    _res = []

    with _connection:
        _cursor = _connection.cursor()
        for _query in _q:
            _cursor.execute(_query)

        for row in _cursor:
            _res.append(row)

    return _res, [_data[0] for _data in _cursor.description]


def get_color(_hash: int) -> str:
    _norm = abs(_hash) * (256 ** 3) / (2 ** (sys.hash_info.width - 1))
    return f'#{str(hex(round(_norm))).replace("0x", "").upper()}FF'


def create_visualisations(_fname=None, _format='svg'):
    _q1_res, _ = get_query_result([f"DROP VIEW IF EXISTS {view_1_name}", query_1, f"SELECT * FROM {view_1_name}"])
    _q2_res, _ = get_query_result([f"DROP VIEW IF EXISTS {view_2_name}", query_2, f"SELECT * FROM {view_2_name}"])
    _q3_res, _ = get_query_result([f"DROP VIEW IF EXISTS {view_3_name}", query_3, f"SELECT * FROM {view_3_name}"])

    _figure, (_bar_ax, _pie_ax, _graph_ax) = plt.subplots(1, 3)

    _bar_ax.bar(range(len(_q1_res)), height=[_item[1] for _item in _q1_res])
    _bar_ax.set_title('Number of AMD CPUs released by year')
    _bar_ax.set_xlabel('Year')
    _bar_ax.set_ylabel('Number of releases')
    _bar_ax.set_xticks(range(len(_q1_res)))
    _bar_ax.set_xticklabels([str(_item[0]) for _item in _q1_res], rotation=45)

    _pie_ax.pie([_item[1] for _item in _q2_res], labels=[str(_item[0]) for _item in _q2_res],
                autopct=lambda p: '{:.1f}%; {:,.0f}'.format(p, p * sum([_item[1] for _item in _q2_res]) / 100),
                textprops={'fontsize': 5},
                colors=[get_color(hash(_item)) for _item in _q2_res],
                radius=1.15)
    _pie_ax.set_title('Total threads count distribution among AMD CPUs:')

    _graph_ax.plot([int(_item[0]) for _item in _q3_res], [int(_item[1]) for _item in _q3_res], marker='^')
    _graph_ax.set_xlabel('Year')
    _graph_ax.set_ylabel('Lithography size, nm')
    _graph_ax.set_title('Minimal lithography size available')

    _mng = plt.get_current_fig_manager()
    _mng.resize(5000, 2000)
    plt.subplots_adjust(left=0.1, bottom=0.15, right=0.975, top=0.9, wspace=0.4, hspace=0.4)

    if _fname is not None:
        plt.tight_layout()
        plt.gcf().set_size_inches(15, 5)
        plt.savefig(_fname + f".{_format}", dpi=500, format=_format)

    plt.show()
