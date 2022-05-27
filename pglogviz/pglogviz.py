import re
import enum
import pydantic
import collections
import random
import colorsys
import json
import datetime
import sys
import dateutil.parser
import pytz

from typing import Optional

TZMAPPING = {
    "CET": pytz.timezone("Europe/Oslo"),
    "CEST": pytz.timezone("Europe/Oslo"),
}

def parse_date(s: str) -> datetime.datetime:
    return dateutil.parser.parse(s, tzinfos=TZMAPPING)

# log_line_prefix from postgresql.conf
# Keys refer to src/backend/utils/error/elog.c
# %t -- time in %Y-%m-%d %H:%M:%S %Z format
# %p -- process ID
# %l -- log line number (within process)
# %u -- username
# %d -- database name
LOG_LINE_PREFIX = "%t [%p-%l] %q%u@%d"

LOG_PREFIX_RE = re.compile(
    r"^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} [A-Za-z]+) \[([0-9]+)-([0-9]+)\] ([A-Za-z0-9]+)@([A-Za-z0-9]+)$"
)
DURATION_LINE_RE = re.compile(r"^([0-9]+[.][0-9]{3}) ms +statement: (.*)$")

class LogLine(pydantic.BaseModel):
    timestamp: Optional[datetime.datetime] = None
    line: str


class LogPrefixInfo(pydantic.BaseModel):
    timestamp: datetime.datetime
    pid: int
    log_line_no: int
    username: str
    database: str


class DurationLogEntry(pydantic.BaseModel):
    duration_usec: int
    statement: str


class HoldingLockLogEntry(pydantic.BaseModel):
    holding_lock_pid: int
    wait_queue_pid: tuple[int, ...]


def parse_holding_lock_log_line(s: str) -> HoldingLockLogEntry:
    first_pid, _, remaining_pids = s.strip().strip(".").partition(". Wait queue: ")

    return HoldingLockLogEntry(
        holding_lock_pid=int(first_pid),
        wait_queue_pid=tuple(int(x) for x in remaining_pids.split(",") if x),
    )


class Statement(pydantic.BaseModel):
    start_time: datetime.datetime
    end_time: datetime.datetime
    pid: int
    log_line_no: int
    statement: str
    duration: float


class EventType(str, enum.Enum):
    DISCONNECT = "disconnect"
    AUTHORIZED = "connection authorized"
    DEADLOCK = "deadlock"
    WAITING_FOR_LOCK = "waiting for lock"


class Event(pydantic.BaseModel):
    time: datetime.datetime
    pid: int
    context: LogPrefixInfo
    event_type: EventType
    primary_related_pids: tuple[int, ...] = ()
    secondary_related_pids: tuple[int, ...] = ()


class Process(pydantic.BaseModel):
    pid: int


EVENT_COLOURS = {
    EventType.DISCONNECT: "magenta",
    EventType.AUTHORIZED: "green",
    EventType.DEADLOCK: "red",
    EventType.WAITING_FOR_LOCK: "black",
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
    hue = rng.random()
    saturation = 1.0
    value = 1.0
    return "#" + "".join(
        [
            "{:02X}".format(int(x * 255))
            for x in colorsys.hsv_to_rgb(hue, saturation, value)
        ]
    )


def parse_log_prefix(prefix: str) -> LogPrefixInfo:
    prefix = prefix.strip()

    m = LOG_PREFIX_RE.match(prefix)
    if not m:
        raise RuntimeError(f"Malformed log prefix: {prefix}")

    timestamp = m.group(1)
    process_id = m.group(2)
    log_line_no = m.group(3)
    username = m.group(4)
    database = m.group(5)

    return LogPrefixInfo(
        timestamp=parse_date(timestamp),
        pid=int(process_id),
        log_line_no=int(log_line_no),
        username=username,
        database=database,
    )


def parse_duration_log_line(line: str) -> Optional[DurationLogEntry]:
    line = line.strip()

    m = DURATION_LINE_RE.match(line)
    if not m:
        return None

    duration_usec = m.group(1).replace(".", "")
    statement = m.group(2)

    return DurationLogEntry(
        duration_usec=int(duration_usec),
        statement=statement,
    )


def create_statement(context: LogPrefixInfo, entry: DurationLogEntry) -> Statement:
    duration = datetime.timedelta(microseconds=entry.duration_usec)
    t1 = context.timestamp
    t0 = context.timestamp - duration

    return Statement(
        start_time=t0,
        end_time=t1,
        duration=duration.total_seconds(),
        pid=context.pid,
        log_line_no=context.log_line_no,
        statement=entry.statement,
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


def render_event_mouseover_content(evt: Event) -> str:
    return f"""
<p>
  <b>Time</b>: {evt.time.isoformat()}
</p>

<p>
  <b>Event</b>: {evt.event_type}
</p>
""".strip()


def render_statement_mouseover_content(stmt: Statement) -> str:
    t0 = stmt.start_time
    t1 = stmt.end_time

    duration = stmt.end_time - stmt.start_time

    return f"""
<p>
  <b>Start</b>: {t0.isoformat()}
</p>

<p>
  <b>End</b>: {t1.isoformat()}
</p>

<p>
  <b>PID</b>: {stmt.pid}
</p>

<p>
  <b>Duration</b>: {duration}
</p>

<p>
  {stmt.statement}
</p>
""".strip()


def visualize(model: Model):
    statements = list(model.statements)
    pids = {process.pid: i for i, process in enumerate(model.processes)}

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

    process_items = []
    for pid, index in pids.items():
        rec.processes.append(
            ProcessVizData(
                id=f"process_{pid}",
                y=index * bar_height,
                height=bar_height,
            )
        )

    event_items = []
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

    statement_items = []
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
    print(rendered)


def ingest_logs_google_json(data):
    records = json.loads(data)

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
        line = line.strip()

        if not line:
            continue

        rv.append(LogLine(line=line))

    return rv

def parse_postgres_lines(lines: list[LogLine]) -> Model:
    stmts_by_process = collections.defaultdict(list)
    events = []

    pids = set()

    def handle_duration(context, core):
        parsed = parse_duration_log_line(core)
        assert parsed

        stmt = create_statement(context, parsed)
        assert stmt.pid

        stmts_by_process[stmt.pid].append(stmt)

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

        pids.add(entry.holding_lock_pid)
        for pid in entry.wait_queue_pid:
            pids.add(pid)

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
    }

    for line in lines:
        handled = False

        for key, func in dispatch.items():
            if key in line.line:
                prefix, _, core = line.line.partition(key)

                context = parse_log_prefix(prefix)
                if line.timestamp:
                    context.timestamp = line.timestamp

                if context.pid:
                    pids.add(context.pid)

                func(context, core)
                handled = True
                break

    processes = []
    for pid in sorted(pids):
        processes.append(
            Process(
                pid=pid,
            )
        )

    return Model(
        statements=[stmt for stmts in stmts_by_process.values() for stmt in stmts],
        events=events,
        processes=processes,
    )


if __name__ == "__main__":
    stmts = []

    data = sys.stdin.read()
    lines = ingest_logs_google_json(data)
    model = parse_postgres_lines(lines)
    visualize(model)
