from .pglogviz import *

TINY_LOG_DATA = """
2022-05-22 10:50:29 CEST [2929634-1] [unknown]@[unknown] LOG:  connection received: host=1.2.3.4 port=37562
2022-05-22 10:50:29 CEST [2929634-2] foo@foo LOG:  connection authorized: user=foo database=foo
2022-05-22 10:50:29 CEST [2929626-2] LOG:  automatic analyze of table "foo" system usage: CPU: user: 0.00 s, system: 0.01 s, elapsed: 1.18 s
2022-05-22 10:50:43 CEST [2864876-56] foo@foo LOG:  duration: 1074.754 ms  statement: SELECT stuff FROM mytbl WHERE x = 1
2022-05-22 10:50:52 CEST [2909723-29] foo@foo DETAIL:  Process holding the lock: 2864876. Wait queue: 2909723.
2022-05-22 10:51:37 CEST [2832965-77] foo@foo LOG:  disconnection: session time: 1:03:43.040 user=foo database=foo host=1.2.3.4 port=48722
some other noise
2022-05-22 10:53:25 CEST [2856945-95] foo@foo ERROR:  deadlock detected
2022-05-22 10:54:29 CEST [2902979-11] foo@foo DETAIL:  Process holding the lock: 2852850. Wait queue: .
2022-05-22 10:57:42 CEST [2857466-96] foo@foo DETAIL:  Process holding the lock: 2845932. Wait queue: 2864876, 2857466.
"""


def test_parse_holding_lock_log_line():
    entry = parse_holding_lock_log_line("2845932. Wait queue: 2864876, 2857466.")
    assert entry == HoldingLockLogEntry(
        holding_lock_pid=2845932,
        wait_queue_pid=(
            2864876,
            2857466,
        ),
    )


def test_parse_log_prefix_old_style():
    prefix = """2022-05-22 11:09:34 CEST [2949465-9] username@database """
    entry = parse_log_prefix(prefix)
    assert entry.pid == 2949465
    assert entry.log_line_no == 9
    assert entry.username == "username"
    assert entry.database == "database"
    oslo = pytz.timezone("Europe/Oslo")
    assert entry.timestamp == datetime.datetime(2022, 5, 22, 11, 9, 34, tzinfo=oslo)


def test_parse_log_prefix_new_style():
    prefix = """2022-05-22 11:09:34 CEST [1011111-38] username@database (foo@bar-quux-default-123456-abc78)"""
    entry = parse_log_prefix(prefix)
    assert entry.pid == 1011111
    assert entry.log_line_no == 38
    assert entry.username == "username"
    assert entry.database == "database"
    assert entry.application_name == "foo@bar-quux-default-123456-abc78"
    oslo = pytz.timezone("Europe/Oslo")
    assert entry.timestamp == datetime.datetime(2022, 5, 22, 11, 9, 34, tzinfo=oslo)


def test_parse_tiny_log():
    model = parse_postgres_lines(split_simple_lines(TINY_LOG_DATA))
    assert len(model.events) == 6
    assert len(model.statements) == 1
    assert len(model.processes) == 9


def test_visualize_tiny_log():
    model = parse_postgres_lines(split_simple_lines(TINY_LOG_DATA))
    visualize(model)


def test_parse_holding_lock_log_line():
    entry = parse_holding_lock_log_line("2845932. Wait queue: 2864876, 2857466.")
    assert entry == HoldingLockLogEntry(
        holding_lock_pid=2845932,
        wait_queue_pid=(
            2864876,
            2857466,
        ),
    )
