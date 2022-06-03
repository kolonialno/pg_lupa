import collections
import colorsys
import datetime
import enum
import json
import random
import re
import string
import typing
from typing import Callable, Iterator, Optional

import dateutil.parser
import dateutil.tz
import jinja2
import pkg_resources
import pydantic

__version__ = "0.0.2"


def format_duration(d: datetime.timedelta) -> str:
    remaining = d.total_seconds()

    days = int(remaining // 86400)
    remaining -= days * 86400

    hours = int(remaining // 3600)
    remaining -= hours * 3600

    minutes = int(remaining // 60)
    remaining -= minutes * 60

    seconds = remaining

    comp = []

    if days:
        comp.append(f"{days}d")

    if hours:
        comp.append(f"{hours}h")

    if minutes:
        comp.append(f"{minutes}m")

    if seconds:
        if int(seconds) == seconds:
            comp.append(f"{int(seconds)}s")
        else:
            comp.append(f"{seconds:.03f}s")

    duration_string = "".join(comp)
    total_millis = int(d.total_seconds() * 1000)

    return f"{duration_string} ({total_millis}ms)"


def contrast_ratio_with_white(rgb) -> float:
    def f(x):
        return x / 3294 if x <= 10 else (x / 269 + 0.0513) ** 2.4

    def relative_luminosity(r, g, b):
        return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b)

    r, g, b = rgb
    return 1 / (relative_luminosity(r, g, b) + 0.05)


def sufficient_contrast_with_white(rgb) -> bool:
    return contrast_ratio_with_white(rgb) > 1.5


def parse_timestamp(s: str) -> datetime.datetime:
    if s.endswith(" CEST"):
        s = s.replace(" CEST", "+02:00")

    if s.endswith(" CET"):
        s = s.replace(" CET", "+01:00")

    return dateutil.parser.parse(s)


DURATION_LINE_RE = re.compile(r"^([0-9]+[.][0-9]{3}) ms +statement:(.*)$")
DURATION_LINE_WITHOUT_STATEMENT_RE = re.compile(r"^([0-9]+[.][0-9]{3}) ms$")


class LogLine(pydantic.BaseModel):
    timestamp: Optional[datetime.datetime] = None
    line: str


class LogPrefixInfo(pydantic.BaseModel):
    timestamp: datetime.datetime
    pid: int
    log_line_no: Optional[int] = None
    username: Optional[str] = None
    database: Optional[str] = None
    application_name: Optional[str] = None


class DataRow(pydantic.BaseModel):
    label: str
    value: str


class DataTable(pydantic.BaseModel):
    rows: Optional[list[DataRow]] = None
    text: Optional[str] = None


def _make_prefix_regex(
    log_prefix_format: str,
) -> str:
    # log_line_prefix from postgresql.conf
    re_comp = ["^"]

    closers: list[str] = []

    okay_regex_literals = set(string.ascii_letters + string.digits + " ")

    already_used_percent_codes = set()

    process_letter: list[Callable[[str], None]] = []

    def add_literal_char(ch):
        if ch in okay_regex_literals:
            re_comp.append(ch)
        else:
            re_comp.append("\\" + hex(ord(ch))[1:])

    def add_timestamp_pattern():
        re_comp.append(
            r"""(?P<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]+)? [A-Za-z]+)"""
        )

    def add_pid_pattern():
        re_comp.append(r"""(?P<pid>[0-9]+)""")

    def add_log_line_pattern():
        re_comp.append(r"""(?P<log_line_no>[0-9]+)""")

    def add_username_pattern():
        re_comp.append(r"""(?P<username>[A-Za-z0-9_-]+)""")

    def add_database_pattern():
        re_comp.append(r"""(?P<database>[A-Za-z0-9_-]+)""")

    def add_application_name_pattern():
        re_comp.append(r"""(?P<application_name>.+)""")

    def add_optional_section():
        re_comp.append("(?:")
        closers.append(")?")

    # Keys refer to src/backend/utils/error/elog.c
    percent_codes = {
        "t": add_timestamp_pattern,
        "m": add_timestamp_pattern,
        "p": add_pid_pattern,
        "u": add_username_pattern,
        "d": add_database_pattern,
        "l": add_log_line_pattern,
        "a": add_application_name_pattern,
        "q": add_optional_section,
        "%": lambda: add_literal_char("%"),
    }

    def consume_percent_code(ch):
        if ch in already_used_percent_codes:
            if ch != "%":
                raise ValueError(f"formatting code %{ch} used twice in log_line_prefix")

        try:
            callback = percent_codes[ch]
        except KeyError:
            raise ValueError(f"unknown or unsupported formatting code %{ch}")

        callback()
        process_letter[0] = consume
        already_used_percent_codes.add(ch)

    def consume(ch):
        if ch == "%":
            process_letter[0] = consume_percent_code
        else:
            add_literal_char(ch)

    process_letter.append(consume)

    for letter in log_prefix_format:
        process_letter[0](letter)

    if "p" not in already_used_percent_codes:
        raise ValueError("log_line_prefix pattern must contain %p")

    if "t" not in already_used_percent_codes and "m" not in already_used_percent_codes:
        raise ValueError("log_line_prefix pattern must contain %t or %m")

    for segment in reversed(closers):
        re_comp.append(segment)

    re_comp.append("$")
    return "".join(re_comp)


def _make_prefix_parser(
    log_prefix_format: str,
) -> Callable[[str], Optional[LogPrefixInfo]]:
    return _make_prefix_parser_from_regex(_make_prefix_regex(log_prefix_format))


def _make_prefix_parser_from_regex(
    regex: str,
) -> Callable[[str], Optional[LogPrefixInfo]]:
    compiled_regex = re.compile(regex)

    active_handlers: dict[str, Callable[[str, LogPrefixInfo], None]] = {}

    def handle_timestamp(value: str, info: LogPrefixInfo) -> None:
        info.timestamp = parse_timestamp(value)

    def handle_pid(value: str, info: LogPrefixInfo) -> None:
        info.pid = int(value)

    def handle_log_line_no(value: str, info: LogPrefixInfo) -> None:
        info.log_line_no = int(value)

    def handle_username(value: str, info: LogPrefixInfo) -> None:
        info.username = value

    def handle_database(value: str, info: LogPrefixInfo) -> None:
        info.database = value

    def handle_application_name(value: str, info: LogPrefixInfo) -> None:
        info.application_name = value

    handlers = {
        "timestamp": handle_timestamp,
        "pid": handle_pid,
        "log_line_no": handle_log_line_no,
        "username": handle_username,
        "database": handle_database,
        "application_name": handle_application_name,
    }

    for name in compiled_regex.groupindex:
        try:
            handler = handlers[name]
        except KeyError:
            raise ValueError(f"unknown field: {repr(name)}")
        else:
            active_handlers[name] = handler

    default_pid = 0
    default_datetime = datetime.datetime(1970, 1, 1)

    def apply(s: str) -> Optional[LogPrefixInfo]:
        m = compiled_regex.match(s)
        if not m:
            return None

        return_value = LogPrefixInfo(
            timestamp=default_datetime,
            pid=default_pid,
        )

        for key, h in active_handlers.items():
            value = m.group(key)
            if value:
                h(value, return_value)

        if return_value.timestamp == default_datetime:
            raise RuntimeError(
                "log_line_prefix pattern not valid: didn't capture timestamp"
            )

        if return_value.pid == default_pid:
            raise RuntimeError("log_line_prefix pattern not valid: didn't capture pid")

        return return_value

    return apply


def make_prefix_parser(
    log_line_prefix: str,
) -> Callable[[str], Optional[LogPrefixInfo]]:
    try:
        return _make_prefix_parser(log_line_prefix)
    except ValueError as e:
        raise ValueError(
            f"Invalid log_prefix_line format {repr(log_line_prefix)}: {e}"
        ) from e


class DurationLogEntry(pydantic.BaseModel):
    duration_usec: int
    statement: Optional[str]


class HoldingLockLogEntry(pydantic.BaseModel):
    holding_lock_pid: int
    wait_queue_pid: tuple[int, ...]


def parse_holding_lock_log_line(s: str) -> HoldingLockLogEntry:
    first_pid, _, remaining_pids = (
        get_first_line(s).strip().strip(".").partition(". Wait queue: ")
    )

    return HoldingLockLogEntry(
        holding_lock_pid=int(first_pid),
        wait_queue_pid=tuple(int(x) for x in remaining_pids.split(",") if x),
    )


class Statement(pydantic.BaseModel):
    start_time: datetime.datetime
    end_time: datetime.datetime
    context: LogPrefixInfo
    pid: int
    log_line_no: int
    statement: str
    duration: float


class EventType(str, enum.Enum):
    DISCONNECT = "disconnect"
    AUTHORIZED = "connection authorized"
    DEADLOCK = "deadlock"
    WAITING_FOR_LOCK = "waiting for lock"
    ANALYZE = "analyze"
    VACUUM = "vacuum"


class Event(pydantic.BaseModel):
    time: datetime.datetime
    pid: int
    context: LogPrefixInfo
    event_type: EventType
    description: Optional[str] = None
    primary_related_pids: tuple[int, ...] = ()
    secondary_related_pids: tuple[int, ...] = ()


class Process(pydantic.BaseModel):
    pid: int
    first_appearance: datetime.datetime


class ProcessSortOrder(str, enum.Enum):
    PID = "pid"
    TIME = "time"


class VizOptions(pydantic.BaseModel):
    process_sort_order: ProcessSortOrder = ProcessSortOrder.TIME
    timezone: Optional[str] = None


class ParseOptions(pydantic.BaseModel):
    log_line_prefix_format: Optional[str] = None
    log_line_prefix_regex: Optional[str] = None


EVENT_COLOURS = {
    EventType.DISCONNECT: "magenta",
    EventType.AUTHORIZED: "green",
    EventType.DEADLOCK: "red",
    EventType.WAITING_FOR_LOCK: "black",
    EventType.ANALYZE: "blue",
    EventType.VACUUM: "violet",
}


class Model(pydantic.BaseModel):
    processes: list[Process]
    statements: list[Statement]
    events: list[Event]
    start_time: datetime.datetime
    end_time: datetime.datetime


class ProcessVizData(pydantic.BaseModel):
    id: str
    y: int
    height: int
    pid: int


class StatementVizData(pydantic.BaseModel):
    id: str
    t_offset: float
    y: int
    height: int
    duration: float
    colour: str
    process_element_id: str
    mouseover_content: str


class EventVizData(pydantic.BaseModel):
    id: str
    t_offset: float
    cy: float
    size: float
    colour: str
    primary_related_process_ids: list[str]
    secondary_related_process_ids: list[str]
    mouseover_content: str


class VizData(pydantic.BaseModel):
    title: str
    events: list[EventVizData]
    processes: list[ProcessVizData]
    statements: list[StatementVizData]
    total_duration_seconds: float
    total_height: int
    total_duration_string: str
    start_time_string: str
    end_time_string: str
    start_time_unix_seconds: float
    end_time_unix_seconds: float


def classify_sql(sql: str) -> str:
    sql = sql.strip().lower()

    sql = " ".join(sql.split())  # _Usually_ correct

    if sql.startswith("select "):
        try:
            index = sql.index(" where ")
        except ValueError:
            return sql

        return sql[:index] + "..."

    return sql


def generate_colours(n: int) -> list[str]:
    rng = random.Random()
    rng.seed(12345678)
    offset = rng.random()

    rv: list[str] = []

    for i in range(n):
        hue = (i / n + offset) % 1.0
        saturation = 1.0
        value = 1.0
        rgb = [int(x * 255) for x in colorsys.hsv_to_rgb(hue, saturation, value)]
        rv.append("#" + "".join(["{:02X}".format(x) for x in rgb]))

    return rv


DEFAULT_LOG_PREFIX_MATCHERS = [
    make_prefix_parser("%t [%p-%l] %q%u@%d"),
    make_prefix_parser("%t [%p-%l] %q%u@%d (%a)"),
    make_prefix_parser("%m [%p]"),
]


def parse_log_prefix(
    prefix: str, matchers: Optional[list[Callable]] = None
) -> LogPrefixInfo:
    matchers = matchers or DEFAULT_LOG_PREFIX_MATCHERS

    for matcher in matchers:
        rv = matcher(prefix)
        if rv:
            return rv

        rv = matcher(prefix.strip())
        if rv:
            return rv

    raise ValueError(f"Log prefix {repr(prefix)} not matched by any known pattern")


def parse_duration_log_line(line: str) -> Optional[DurationLogEntry]:
    line = get_first_line(line).strip()

    m = DURATION_LINE_RE.match(line)
    if m:
        duration_usec = m.group(1).replace(".", "")
        statement = m.group(2)

        return DurationLogEntry(
            duration_usec=int(duration_usec),
            statement=statement,
        )

    m = DURATION_LINE_WITHOUT_STATEMENT_RE.match(line)
    if m:
        duration_usec = m.group(1).replace(".", "")

        return DurationLogEntry(
            duration_usec=int(duration_usec),
            statement=None,
        )

    raise RuntimeError(f"Malformed duration log line: {line}")


def create_statement(context: LogPrefixInfo, entry: DurationLogEntry) -> Statement:
    duration = datetime.timedelta(microseconds=entry.duration_usec)
    t1 = context.timestamp
    t0 = context.timestamp - duration

    return Statement(
        start_time=t0,
        end_time=t1,
        context=context,
        duration=duration.total_seconds(),
        pid=context.pid,
        log_line_no=context.log_line_no,
        statement=entry.statement or "",
    )


def make_data_table_renderer() -> Callable[[DataTable], str]:
    env = jinja2.Environment(
        loader=jinja2.FunctionLoader(
            lambda name: pkg_resources.resource_string(
                "pg_lupa.resources", name
            ).decode()
        ),
        autoescape=True,
    )

    tmpl = env.get_template("context.template.html")

    def render_data_table(table: DataTable) -> str:
        return tmpl.render(table=table)

    return render_data_table


def make_data_table(
    context: LogPrefixInfo,
    *,
    rows: Optional[list[DataRow]] = None,
    text: Optional[str] = None,
) -> DataTable:
    table = DataTable(
        rows=[],
        text=text or None,
    )

    assert table.rows is not None

    if context.pid:
        table.rows.append(
            DataRow(
                label="PID",
                value=str(context.pid),
            )
        )

    if context.log_line_no:
        table.rows.append(
            DataRow(
                label="Log line no.",
                value=str(context.log_line_no),
            )
        )

    if context.username:
        table.rows.append(
            DataRow(
                label="Username",
                value=str(context.username),
            )
        )

    if context.database:
        table.rows.append(
            DataRow(
                label="Database",
                value=str(context.database),
            )
        )

    if context.application_name:
        table.rows.append(
            DataRow(
                label="Application",
                value=str(context.application_name),
            )
        )

    table.rows.extend(rows or [])

    return table


def visualize(model: Model, out: typing.TextIO, options: Optional[VizOptions] = None):
    options = options or VizOptions()

    tz = dateutil.tz.gettz(options.timezone) if options.timezone else None

    timestamp_format = "%Y-%m-%d %H:%M:%S.%f %Z"

    def format_datetime(t: datetime.datetime) -> str:
        if tz:
            t = t.astimezone(tz)
        return t.strftime(timestamp_format)

    render_data_table = make_data_table_renderer()

    statements = list(model.statements)

    processes = list(model.processes)
    processes.sort(
        key={
            ProcessSortOrder.PID: lambda p: p.pid,
            ProcessSortOrder.TIME: lambda p: (p.first_appearance, p.pid),
        }[options.process_sort_order]
    )

    pids = {process.pid: i for i, process in enumerate(processes)}

    statements.sort(key=lambda x: x.start_time)

    min_time_dt = min(min(x.start_time for x in statements), model.start_time)
    max_time_dt = model.end_time

    min_time = min_time_dt.timestamp()
    max_time = max_time_dt.timestamp()

    bar_height = 10

    rec = VizData(
        title=f"{format_datetime(min_time_dt)} to {format_datetime(max_time_dt)}",
        statements=[],
        processes=[],
        events=[],
        total_duration_seconds=max_time - min_time,
        total_duration_string=format_duration(max_time_dt - min_time_dt),
        start_time_string=format_datetime(min_time_dt),
        end_time_string=format_datetime(max_time_dt),
        start_time_unix_seconds=min_time_dt.timestamp(),
        end_time_unix_seconds=max_time_dt.timestamp(),
        total_height=len(pids) * bar_height,
    )

    for pid, index in pids.items():
        rec.processes.append(
            ProcessVizData(
                id=f"process_{pid}",
                y=index * bar_height,
                height=bar_height,
                pid=pid,
            )
        )

    for i, evt in enumerate(model.events):
        rec.events.append(
            EventVizData(
                id=f"event_{i+1}",
                t_offset=evt.time.timestamp() - min_time,
                cy=pids[evt.pid] * bar_height + 0.5 * bar_height,
                size=0.4 * bar_height,
                mouseover_content=render_data_table(
                    make_data_table(
                        evt.context,
                        text=evt.description,
                        rows=[
                            DataRow(
                                label="Time",
                                value=format_datetime(evt.time),
                            ),
                            DataRow(
                                label="Event",
                                value=evt.event_type.value,
                            ),
                        ],
                    )
                ),
                colour=EVENT_COLOURS[evt.event_type],
                primary_related_process_ids=[
                    f"process_{pid}" for pid in evt.primary_related_pids
                ],
                secondary_related_process_ids=[
                    f"process_{pid}" for pid in evt.secondary_related_pids
                ],
            )
        )

    classes = set()
    for stmt in statements:
        classes.add(classify_sql(stmt.statement))

    colours = {}
    for cls, col in zip(sorted(classes), generate_colours(len(classes))):
        colours[cls] = col

    for i, stmt in enumerate(statements):
        name = f"stmt{i+1}"
        col = colours[classify_sql(stmt.statement)]
        t0 = stmt.start_time.timestamp()
        t1 = stmt.end_time.timestamp()
        rec.statements.append(
            StatementVizData(
                id=name,
                t_offset=t0 - min_time,
                y=pids[stmt.pid] * bar_height,
                height=bar_height,
                duration=t1 - t0,
                colour=col,
                process_element_id=f"process_{stmt.pid}",
                mouseover_content=render_data_table(
                    make_data_table(
                        stmt.context,
                        text=stmt.statement,
                        rows=[
                            DataRow(
                                label="Start",
                                value=format_datetime(stmt.start_time),
                            ),
                            DataRow(
                                label="End",
                                value=format_datetime(stmt.end_time),
                            ),
                            DataRow(
                                label="Duration",
                                value=format_duration(stmt.end_time - stmt.start_time),
                            ),
                        ],
                    )
                ),
            )
        )

    out.write(render_html(rec))


def render_html(data: VizData) -> str:
    env = jinja2.Environment(
        loader=jinja2.FunctionLoader(
            lambda name: pkg_resources.resource_string(
                "pg_lupa.resources", name
            ).decode()
        ),
        autoescape=True,
    )

    tmpl = env.get_template("lupa.template.html")

    return tmpl.render(
        embeddable_data=data.json(indent=2),
        lupa_version=__version__,
        report=data,
    )


def ingest_logs_google_json(records):
    carriage_return = "\r"

    for record in records:
        line = record["textPayload"]

        timestamp = parse_timestamp(record["timestamp"])

        if carriage_return in line:
            line = line[line.index(carriage_return) + 1 :]

        yield LogLine(
            timestamp=timestamp,
            line=line,
        )


def split_simple_lines(data: str) -> list[LogLine]:
    rv = []

    for line in data.strip().splitlines():
        if not line.strip():
            continue

        rv.append(LogLine(line=line))

    return rv


def get_first_line(full_entry: str) -> str:
    # In multiline logs from many different processes, occasionally lines get intermingled.
    # In single-line contexts, discard any additional lines that may have accidentally
    # attached themselves.
    return full_entry.splitlines()[0]


def parse_postgres_lines(
    lines: list[LogLine], options: Optional[ParseOptions] = None
) -> Model:
    options = options or ParseOptions()

    prefix_matchers = []
    if options.log_line_prefix_format:
        prefix_matchers.append(make_prefix_parser(options.log_line_prefix_format))

    if options.log_line_prefix_regex:
        # The regex will be long and ugly.
        # It's available as a fallback for people who use options not supported by the
        # format-string parser.
        prefix_matchers.append(
            _make_prefix_parser_from_regex(options.log_line_prefix_regex)
        )

    stmts_by_process = collections.defaultdict(list)
    events = []

    pids_by_first_seen: dict[int, datetime.datetime] = {}

    def saw_pid_at(pid: int, t: datetime.datetime):
        try:
            old_time = pids_by_first_seen[pid]
        except KeyError:
            pids_by_first_seen[pid] = t
        else:
            if old_time > t:
                pids_by_first_seen[pid] = t

    def handle_duration(context, core):
        parsed = parse_duration_log_line(core)
        if not parsed:
            raise RuntimeError(f"Unable to parse duration log line: {core}")

        stmt = create_statement(context, parsed)
        assert stmt.pid

        saw_pid_at(stmt.pid, stmt.start_time)

        stmts_by_process[stmt.pid].append(stmt)

    def handle_automatic_analyze(context, core):
        events.append(
            Event(
                time=context.timestamp,
                pid=context.pid,
                context=context,
                event_type=EventType.ANALYZE,
                description=core.strip(),
            )
        )

    def handle_automatic_vacuum(context, core):
        events.append(
            Event(
                time=context.timestamp,
                pid=context.pid,
                context=context,
                event_type=EventType.VACUUM,
                description=core.strip(),
            )
        )

    def handle_disconnection(context, core):
        events.append(
            Event(
                time=context.timestamp,
                pid=context.pid,
                context=context,
                event_type=EventType.DISCONNECT,
            )
        )

    def handle_connection_authorized(context, core):
        events.append(
            Event(
                time=context.timestamp,
                pid=context.pid,
                context=context,
                event_type=EventType.AUTHORIZED,
            )
        )

    def handle_process_holding_lock(context, core):
        entry = parse_holding_lock_log_line(core)

        saw_pid_at(entry.holding_lock_pid, context.timestamp)
        for pid in entry.wait_queue_pid:
            saw_pid_at(pid, context.timestamp)

        wait_queue_except_self = tuple(
            x for x in entry.wait_queue_pid if x != context.pid
        )

        events.append(
            Event(
                time=context.timestamp,
                pid=context.pid,
                context=context,
                event_type=EventType.WAITING_FOR_LOCK,
                primary_related_pids=(entry.holding_lock_pid,),
                secondary_related_pids=wait_queue_except_self,
            )
        )

    def handle_deadlock_detected(context, core):
        events.append(
            Event(
                time=context.timestamp,
                pid=context.pid,
                context=context,
                event_type=EventType.DEADLOCK,
            )
        )

    dispatch = {
        "LOG:  duration: ": handle_duration,
        "LOG:  disconnection: ": handle_disconnection,
        "LOG:  connection authorized: ": handle_connection_authorized,
        "DETAIL:  Process holding the lock: ": handle_process_holding_lock,
        "ERROR:  deadlock detected": handle_deadlock_detected,
        "LOG:  automatic analyze of ": handle_automatic_analyze,
        "LOG:  automatic vacuum of ": handle_automatic_vacuum,
    }

    timestamp_minmax: list[datetime.datetime] = []

    def try_parse(line: LogLine):
        for key, func in dispatch.items():
            if key in line.line:
                prefix, _, core = line.line.partition(key)

                context = parse_log_prefix(prefix, prefix_matchers)
                if line.timestamp:
                    context.timestamp = line.timestamp

                if not timestamp_minmax:
                    timestamp_minmax.append(context.timestamp)
                    timestamp_minmax.append(context.timestamp)
                else:
                    timestamp_minmax[0] = min(timestamp_minmax[0], context.timestamp)
                    timestamp_minmax[1] = max(timestamp_minmax[1], context.timestamp)

                saw_pid_at(context.pid, context.timestamp)

                func(context, core)
                break

    for i, line in enumerate(lines):
        try:
            try_parse(line)
        except Exception as e:
            raise RuntimeError(
                f"Failed to parse log line #{i+1}: {repr(line.line)}"
            ) from e

    processes = []
    for pid in pids_by_first_seen:
        processes.append(
            Process(
                pid=pid,
                first_appearance=pids_by_first_seen[pid],
            )
        )

    return Model(
        statements=[stmt for stmts in stmts_by_process.values() for stmt in stmts],
        events=events,
        processes=processes,
        start_time=timestamp_minmax[0],
        end_time=timestamp_minmax[1],
    )


def merge_continuation_lines(lines: Iterator[LogLine]) -> Iterator[LogLine]:
    last_line: Optional[LogLine] = None

    for line in lines:
        if not line.line.startswith("\t"):
            if last_line:
                yield last_line
            last_line = line.copy()
            continue

        if last_line is None:
            raise RuntimeError("Continuation line without preceding line")

        last_line.line += "\n" + line.line

    if last_line:
        yield last_line


def _parse_unmerged_log_lines(f: typing.TextIO) -> Iterator[LogLine]:
    data = f.read()

    try:
        json_record = json.loads(data)
        yield from ingest_logs_google_json(json_record)
    except json.decoder.JSONDecodeError:
        yield from split_simple_lines(data)


def parse_log_lines_automagically(f: typing.TextIO) -> Iterator[LogLine]:
    yield from merge_continuation_lines(_parse_unmerged_log_lines(f))


def parse_log_data_automagically(
    f: typing.TextIO, options: Optional[ParseOptions] = None
) -> Model:
    lines = parse_log_lines_automagically(f)
    return parse_postgres_lines(list(lines), options)


def run_analyzer(
    *,
    input_file: typing.TextIO,
    output_file: typing.TextIO,
    parse_options: Optional[ParseOptions] = None,
    viz_options: Optional[VizOptions] = None,
) -> None:
    model = parse_log_data_automagically(input_file, parse_options)
    visualize(model, output_file, viz_options)
