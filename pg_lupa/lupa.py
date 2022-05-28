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
import pydantic
import pytz

TZMAPPING = {
    "CET": pytz.timezone("Europe/Oslo"),
    "CEST": pytz.timezone("Europe/Oslo"),
}


def contrast_ratio_with_white(rgb) -> float:
    def f(x):
        return x / 3294 if x <= 10 else (x / 269 + 0.0513) ** 2.4

    def relative_luminosity(r, g, b):
        return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b)

    r, g, b = rgb
    return 1 / (relative_luminosity(r, g, b) + 0.05)


def sufficient_contrast_with_white(rgb) -> bool:
    return contrast_ratio_with_white(rgb) > 1.5


def parse_date(s: str) -> datetime.datetime:
    return dateutil.parser.parse(s, tzinfos=TZMAPPING)


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


def _make_prefix_parser(
    log_prefix_format: str,
) -> Callable[[str], Optional[LogPrefixInfo]]:
    # log_line_prefix from postgresql.conf
    re_comp = ["^"]

    closers: list[str] = []

    okay_regex_literals = set(string.ascii_letters + string.digits + " ")

    already_used_percent_codes = set()
    already_used_fields = set()

    process_letter: list[Callable[[str], None]] = []
    handlers: list[Callable[[re.Match, LogPrefixInfo], None]] = []

    def use_field(field: str):
        if field in already_used_fields:
            raise ValueError(f"cannot use {field} twice")
        already_used_fields.add(field)

    def add_literal_char(ch):
        if ch in okay_regex_literals:
            re_comp.append(ch)
        else:
            re_comp.append("\\" + hex(ord(ch))[1:])

    def add_timestamp_pattern():
        re_comp.append(
            r"""(?P<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]+)? [A-Za-z]+)"""
        )
        use_field("timestamp")

        def capture(m, info):
            info.timestamp = parse_date(m.group("timestamp"))

        handlers.append(capture)

    def add_pid_pattern():
        re_comp.append(r"""(?P<pid>[0-9]+)""")
        use_field("pid")

        def capture(m, info):
            info.pid = int(m.group("pid"))

        handlers.append(capture)

    def add_log_line_pattern():
        re_comp.append(r"""(?P<log_line>[0-9]+)""")
        use_field("log_line")

        def capture(m, info):
            info.log_line_no = int(m.group("log_line"))

        handlers.append(capture)

    def add_username_pattern():
        re_comp.append(r"""(?P<username>[A-Za-z0-9_-]+)""")
        use_field("username")

        def capture(m, info):
            info.username = m.group("username")

        handlers.append(capture)

    def add_database_pattern():
        re_comp.append(r"""(?P<database>[A-Za-z0-9_-]+)""")
        use_field("database")

        def capture(m, info):
            info.database = m.group("database")

        handlers.append(capture)

    def add_application_name_pattern():
        re_comp.append(r"""(?P<application_name>.+)""")
        use_field("application_name")

        def capture(m, info):
            info.application_name = m.group("application_name")

        handlers.append(capture)

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

    compilable_regex = "".join(re_comp)

    compiled_regex = re.compile(compilable_regex)

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

        for h in handlers:
            h(m, return_value)

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


class ParseOptions(pydantic.BaseModel):
    log_line_prefix_format: Optional[str] = None


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


class ProcessVizData(pydantic.BaseModel):
    id: str
    y: int
    height: int


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
    events: list[EventVizData]
    processes: list[ProcessVizData]
    statements: list[StatementVizData]
    total_duration: float
    total_height: int


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


def hash_as_colour(s: str) -> str:
    rng = random.Random()
    rng.seed(s)
    for _ in range(100):
        hue = rng.random()
        saturation = 1.0
        value = 1.0
        rgb = [int(x * 255) for x in colorsys.hsv_to_rgb(hue, saturation, value)]
        if sufficient_contrast_with_white(rgb):
            break
    return "#" + "".join(["{:02X}".format(x) for x in rgb])


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


template = """
<html>
<head>
<style>

div.tooltip {
     position: absolute;
     text-align: left;
     padding: .5rem;
     background: #FFFFFF;
     color: #313639;
     border: 1px solid #313639;
     border-radius: 4px;
     pointer-events: none;
     font-size: 1.3rem;
}

#context-info {
    position: fixed;
    top: 0;
    left: 0;
    width: 30vw;
    height: 100vh;
    overflow-y: auto;
}

#timeline {
    position: fixed;
    top: 0;
    left: 30vw;
    width: 70vw;
    height: 100vh;
    overflow-y: auto;
}

.pg_process {
    display: none;
}

.process-primary-highlight {
    display: inline-block;
    fill: rgb(240,180,180);
}

.process-secondary-highlight {
    display: inline-block;
    fill: rgb(200,200,200);
}

.context-highlight {
    stroke: rgb(255, 200, 200);
    stroke-width: 3px;
}

</style>
</head>
<body>
  <div id="context-info"></div>
  <div id="timeline"></div>
</body>

<script id="data" type="application/json">
%JSON_DATA%
</script>

<script src="https://cdn.jsdelivr.net/npm/jquery@3.2.1/dist/jquery.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script>

let contentLockedToID = null;

function setContent(id, content, lock, processes1, processes2) {
  if (contentLockedToID && !lock) return;

  const alreadyLockedToThis = contentLockedToID && contentLockedToID === id;
  if (alreadyLockedToThis) {
    $("#context-info").html("");
    $(".context-highlight").removeClass("context-highlight");
    $(".process-highlight").removeClass("process-primary-highlight process-secondary-highlight");
    contentLockedToID = null;
    return;
  }

  $("#context-info").html(content);
  contentLockedToID = lock ? id : null;

  $(".context-highlight").removeClass("context-highlight");
  $("#" + id).addClass("context-highlight");

  $(".process-highlight").removeClass("process-primary-highlight process-secondary-highlight");
  if (processes1) {
      processes1.forEach(procid => {
        $(document.getElementById(procid)).addClass("process-highlight process-primary-highlight");
      });
  }
  if (processes2) {
    processes2.forEach(procid => {
      $(document.getElementById(procid)).addClass("process-highlight process-secondary-highlight");
    });
  }
}

function draw() {
  const width = $("#timeline").width() - 20;
  const data = JSON.parse(document.getElementById("data").textContent);

  const div = d3.select("body").append("div")
     .attr("class", "tooltip")
     .style("opacity", 0);

  const svg = d3.select("#timeline")
    .append("svg")
        .attr("width", width)
        .attr("height", data.total_height);

  svg.selectAll().data(data.processes)
    .enter().append("rect")
      .attr("id", function(d) { return d.id; })
      .attr("x", 0 )
      .attr("width", width )
      .attr("y", function(d) { return d.y; })
      .attr("height", function(d) { return d.height; })
      .attr("class", "pg_process")
      ;

  svg.selectAll().data(data.statements)
    .enter().append("rect")
      .attr("id", function(d) { return d.id; })
      .attr("class", "pg_stmt")
      .attr("x", function(d) { return d.t_offset / data.total_duration * width; })
      .attr("y", function(d) { return d.y; })
      .attr("width", function(d) { return d.duration / data.total_duration * width; })
      .attr("height", function(d) { return d.height; })
      .style("fill", function(d) { return d.colour; })
      .on("click", function(evt, d) {
        setContent(d.id, d.mouseover_content, true);
      })
      .on("mouseover", function(evt, d) {
        setContent(d.id, d.mouseover_content, false);
      })
      .on("mouseout", function(d) {
        setContent(null, "", false);
      })
      ;

  svg.selectAll().data(data.events)
    .enter().append("circle")
      .attr("id", function(d) { return d.id; })
      .attr("cx", function(d) {
        return d.t_offset / data.total_duration * width;
      })
      .attr("fill", function(d) { return d.colour; })
      .attr("cy", function(d) { return d.cy; })
      .attr("r", function(d) { return d.size; })
      .attr("class", "pg_event")
      .on("click", function(evt, d) {
        setContent(d.id, d.mouseover_content, true, d.primary_related_process_ids, d.secondary_related_process_ids);
      })
      .on("mouseover", function(evt, d) {
        setContent(d.id, d.mouseover_content, false, d.primary_related_process_ids, d.secondary_related_process_ids);
      })
      .on("mouseout", function(d) {
        setContent(null, "", false);
      })
      ;

}

draw();

</script>
"""


def render_context_table(context: LogPrefixInfo) -> str:
    comp = []

    if context.pid:
        comp.append(f"<p><b>PID</b>: {context.pid}</p>")

    if context.log_line_no:
        comp.append(f"<p><b>Log line no.</b>: {context.log_line_no}</p>")

    if context.username:
        comp.append(f"<p><b>Username</b>: {context.username}</p>")

    if context.database:
        comp.append(f"<p><b>Database</b>: {context.database}</p>")

    if context.application_name:
        comp.append(f"<p><b>Application</b>: {context.application_name}</p>")

    return "\n".join(comp)


def render_event_mouseover_content(evt: Event) -> str:
    desc = ""
    if evt.description:
        desc = f"""
<p>
  {evt.description}
</p>
"""

    return (
        render_context_table(evt.context)
        + f"""
<p>
  <b>Time</b>: {evt.time.isoformat()}
</p>

<p>
  <b>Event</b>: {evt.event_type}
</p>
""".strip()
        + desc
    )


def render_statement_mouseover_content(stmt: Statement) -> str:
    t0 = stmt.start_time
    t1 = stmt.end_time

    duration = stmt.end_time - stmt.start_time

    return (
        render_context_table(stmt.context)
        + f"""
<p>
  <b>Start</b>: {t0.isoformat()}
</p>

<p>
  <b>End</b>: {t1.isoformat()}
</p>

<p>
  <b>Duration</b>: {duration}
</p>

<p>
  {stmt.statement}
</p>
""".strip()
    )


def visualize(model: Model, out: typing.TextIO, options: Optional[VizOptions] = None):
    options = options or VizOptions()

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

    min_time = min(x.start_time for x in statements).timestamp()
    max_time = max(x.end_time for x in statements).timestamp()

    bar_height = 10

    rec = VizData(
        statements=[],
        processes=[],
        events=[],
        total_duration=max_time - min_time,
        total_height=len(pids) * bar_height,
    )

    for pid, index in pids.items():
        rec.processes.append(
            ProcessVizData(
                id=f"process_{pid}",
                y=index * bar_height,
                height=bar_height,
            )
        )

    for i, evt in enumerate(model.events):
        rec.events.append(
            EventVizData(
                id=f"event_{i+1}",
                t_offset=evt.time.timestamp() - min_time,
                cy=pids[evt.pid] * bar_height + 0.5 * bar_height,
                size=0.5 * 0.5 * bar_height,
                mouseover_content=render_event_mouseover_content(evt),
                colour=EVENT_COLOURS[evt.event_type],
                primary_related_process_ids=[
                    f"process_{pid}" for pid in evt.primary_related_pids
                ],
                secondary_related_process_ids=[
                    f"process_{pid}" for pid in evt.secondary_related_pids
                ],
            )
        )

    for i, stmt in enumerate(statements):
        name = f"stmt{i+1}"
        col = hash_as_colour(classify_sql(stmt.statement))
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
                mouseover_content=render_statement_mouseover_content(stmt),
            )
        )

    json_data = rec.json(indent=2)
    rendered = template.replace("%JSON_DATA%", json_data)
    print(rendered, file=out)


def ingest_logs_google_json(records):
    carriage_return = "\r"

    for record in records:
        line = record["textPayload"]

        timestamp = parse_date(record["timestamp"])

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

    def try_parse(line: LogLine):
        for key, func in dispatch.items():
            if key in line.line:
                prefix, _, core = line.line.partition(key)

                context = parse_log_prefix(prefix, prefix_matchers)
                if line.timestamp:
                    context.timestamp = line.timestamp

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
