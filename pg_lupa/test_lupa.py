import datetime
import io

import pytest
import pytz

from .lupa import (
    HoldingLockLogEntry,
    classify_sql,
    make_prefix_parser,
    parse_duration_log_line,
    parse_holding_lock_log_line,
    parse_log_lines_automagically,
    parse_log_prefix,
    parse_postgres_lines,
    split_simple_lines,
    visualize,
)

TINY_LOG_DATA = """
2022-05-22 10:50:29 CEST [2929634-1] [unknown]@[unknown] LOG:  connection received: host=1.2.3.4 port=37562
2022-05-22 10:50:29 CEST [2929634-2] hello-world@foo_bar LOG:  connection authorized: user=foo database=foo
2022-05-22 10:50:29 CEST [2929626-2] LOG:  automatic analyze of table "foo" system usage: CPU: user: 0.00 s, system: 0.01 s, elapsed: 1.18 s
2022-05-22 10:50:43 CEST [2864876-56] foo@foo LOG:  duration: 1074.754 ms  statement: SELECT stuff FROM mytbl WHERE x = 1
2022-05-22 10:50:52 CEST [2909723-29] foo@foo DETAIL:  Process holding the lock: 2864876. Wait queue: 2909723.
2022-05-22 10:51:37 CEST [2832965-77] foo@foo LOG:  disconnection: session time: 1:03:43.040 user=foo database=foo host=1.2.3.4 port=48722
some other noise
2022-05-22 10:53:25 CEST [2856945-95] foo@foo ERROR:  deadlock detected
2022-05-22 10:54:29 CEST [2902979-11] foo@foo DETAIL:  Process holding the lock: 2852850. Wait queue: .
2022-05-22 10:57:42 CEST [2857466-96] foo@foo DETAIL:  Process holding the lock: 2845932. Wait queue: 2864876, 2857466.
"""

SLIGHTLY_CORRUPT_LOG_DATA = """
2022-05-22 10:50:29 CEST [2929634-1] [unknown]@[unknown] LOG:  connection received: host=1.2.3.4 port=37562
\thello world
2022-05-22 10:50:29 CEST [2929634-2] hello-world@foo_bar LOG:  connection authorized: user=foo database=foo
\thello world
2022-05-22 10:50:29 CEST [2929626-2] LOG:  automatic analyze of table "foo" system usage: CPU: user: 0.00 s, system: 0.01 s, elapsed: 1.18 s
\thello world
2022-05-22 10:50:43 CEST [2864876-56] foo@foo LOG:  duration: 1074.754 ms  statement: SELECT stuff FROM mytbl WHERE x = 1
\thello world
2022-05-22 10:50:52 CEST [2909723-29] foo@foo DETAIL:  Process holding the lock: 2864876. Wait queue: 2909723.
\thello world
2022-05-22 10:51:37 CEST [2832965-77] foo@foo LOG:  disconnection: session time: 1:03:43.040 user=foo database=foo host=1.2.3.4 port=48722
\thello world
2022-05-22 10:53:25 CEST [2856945-95] foo@foo ERROR:  deadlock detected
\thello world
2022-05-22 10:54:29 CEST [2902979-11] foo@foo DETAIL:  Process holding the lock: 2852850. Wait queue: .
\thello world
2022-05-22 10:57:42 CEST [2857466-96] foo@foo DETAIL:  Process holding the lock: 2845932. Wait queue: 2864876, 2857466.
\thello world
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


def test_parse_log_prefix_default_style():
    prefix = "2022-05-22 10:50:29.123 CEST [2929634] "
    entry = parse_log_prefix(prefix)
    assert entry.pid == 2929634
    assert entry.log_line_no is None
    assert entry.username is None
    assert entry.database is None
    assert entry.application_name is None
    oslo = pytz.timezone("Europe/Oslo")
    assert entry.timestamp == datetime.datetime(
        2022, 5, 22, 10, 50, 29, 123000, tzinfo=oslo
    )


def test_parse_log_prefix_truncated_style():
    prefix = """2022-05-22 11:09:34 CEST [2949465-9] """
    entry = parse_log_prefix(prefix)
    assert entry.pid == 2949465
    assert entry.log_line_no == 9
    assert entry.username is None
    assert entry.database is None
    oslo = pytz.timezone("Europe/Oslo")
    assert entry.timestamp == datetime.datetime(2022, 5, 22, 11, 9, 34, tzinfo=oslo)


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
    assert len(model.events) == 7
    assert len(model.statements) == 1
    assert len(model.processes) == 10


def test_visualize_tiny_log():
    model = parse_postgres_lines(split_simple_lines(TINY_LOG_DATA))
    visualize(model, io.StringIO())


def test_visualize_slightly_corrupt_tiny_log():
    model = parse_postgres_lines(split_simple_lines(SLIGHTLY_CORRUPT_LOG_DATA))
    visualize(model, io.StringIO())


def test_classify_sql():
    one = "SELECT foo FROM bar WHERE quux = 99"
    two = "SELECT foo FROM bar WHERE quux = 123"
    three = "SELECT foo, quux FROM bar WHERE quux = 123"
    four = "SELECT foo   FROM   bar   WHERE  quux = 99  "
    assert classify_sql(one) == classify_sql(four)
    assert classify_sql(one) == classify_sql(two)
    assert classify_sql(one) != classify_sql(three)


def test_parse_duration_line_with_extra_noise():
    assert parse_duration_log_line(
        """\
1068.012 ms  statement: SELECT this FROM that ASC LIMIT 1
\tsystem usage: CPU: user: 0.02 s, system: 0.01 s, elapsed: 3.91 s
\tavg read rate: 2.940 MB/s, avg write rate: 0.044 MB/s
\tbuffer usage: 4558 hits, 1474 misses, 22 dirtied
\ttuples: 1756 removed, 2658 remain, 0 are dead but not yet removable, oldest xmin: 3191051515
\tpages: 0 removed, 4574 remain, 0 skipped due to pins, 2785 skipped frozen\
"""
    )


def test_continuation_lines():
    testdata = """\
2022-05-22 10:50:29 CEST [2929626-2] log line one
2022-05-22 10:50:29 CEST [2929626-2] log line two
\tcontinuation data
\tmore data
2022-05-22 10:50:29 CEST [2929626-2] log line three
"""
    lines = list(parse_log_lines_automagically(io.StringIO(testdata)))
    assert len(lines) == 3
    assert lines[0].line == "2022-05-22 10:50:29 CEST [2929626-2] log line one"
    assert (
        lines[1].line
        == "2022-05-22 10:50:29 CEST [2929626-2] log line two\n\tcontinuation data\n\tmore data"
    )
    assert lines[2].line == "2022-05-22 10:50:29 CEST [2929626-2] log line three"


def test_constructed_matchers_make_default():
    matcher = make_prefix_parser("%m [%p] ")
    assert matcher("2022-05-22 10:50:29.123 CEST [2929634] ")
    assert matcher("2022-05-22 10:50:29.123 CEST [2929634] ").pid == 2929634
    assert matcher("2022-05-22 10:50:29.123 CEST [2929634] ").timestamp.month == 5


def test_invalid_make_matchers():
    with pytest.raises(ValueError):
        make_prefix_parser("%m %p %m %p")

    with pytest.raises(ValueError):
        make_prefix_parser("%m")

    with pytest.raises(ValueError):
        make_prefix_parser("%!")


def test_constructed_matchers_make_old_style():
    matcher = make_prefix_parser("%t [%p-%l] %q%u@%d")
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar")
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar").username == "foo"
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar").database == "bar"
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar").pid == 2929634
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar").log_line_no == 1
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar").timestamp.month == 5
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ")
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").username is None
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").database is None
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").pid == 2929634
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").log_line_no == 1
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").timestamp.month == 5


def test_constructed_matchers_make_new_style():
    matcher = make_prefix_parser("%t [%p-%l] %q%u@%d (%a)")
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar (x)")
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar (x)").username == "foo"
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar (x)").database == "bar"
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar (x)").pid == 2929634
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar (x)").log_line_no == 1
    assert (
        matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar (x)").timestamp.month == 5
    )
    assert (
        matcher("2022-05-22 10:50:29 CEST [2929634-1] foo@bar (x)").application_name
        == "x"
    )
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ")
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").username is None
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").database is None
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").pid == 2929634
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").log_line_no == 1
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").timestamp.month == 5
    assert matcher("2022-05-22 10:50:29 CEST [2929634-1] ").application_name is None


def test_constructed_matchers_fancy_application_name():
    matcher = make_prefix_parser("%t %p <%a> %l")
    parsed = matcher(
        "2022-05-22 10:50:29 CEST 1 <hello world! this is my fancy app name...? :>> 2"
    )
    assert parsed.application_name == "hello world! this is my fancy app name...? :>"
