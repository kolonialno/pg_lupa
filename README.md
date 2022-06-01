# 🔍 Lupa

Lupa visualizes PostgreSQL logs.

![Screenshot](/docs/screenshot.png)

It can be hard to debug Postgres issues based on manually reading logs.
Problems that arise are often not indicated by single "smoking gun" log
entries. Query A was slow because it was waiting for query B -- but why
was query B slow? Tracing a sequence of cause-and-effect through the
logs can be a tedious exercise, especially if you're not 100% sure
about what you're looking for.

Lupa helps solve this problem by visualizing your logs so you can use
visual pattern recognition to identify what you're looking at.
Hand it a few megabytes of database logs covering a period of interest
(e.g. an outage), and it'll generate an interactive timeline report.

## Running Lupa

```
$ poetry run python -m pg_lupa --input-logs logfile.log > my-report.html
```

Note that you probably will not want to use your entire
`/var/log/postgresql/postgresql-NN-main.log` log file as input.
Lupa works best if you give it an extract of the logs focusing of the
time period of interest -- a few tens of megabytes of logs at most.

If you have a lot of logs, e.g. if you're running with
`log_min_duration_statement=0`, you may also wish to do some preprocessing
to cut filter out log lines that you know are uninteresting.

## Assumptions about input logs

Lupa needs to be aware of your `log_line_prefix`, which is a setting in
your Postgres configuration. "%p" (process PID) must be present,
otherwise Lupa will not be able to make a sensible visualization.
Luckily, the default value of `%m [%p]` includes it.

You can pass this as a flag with `--log-line-prefix-format`. Lupa will
use the Postgres formatting string to construct a regex with which
to parse log prefixes. However, Lupa doesn't understand all possible
formatting directives yet. If your format is not supported yet, you can use
`--log-line-prefix-regex` to specify a regex directly. It must have
named capturing groups for at least "timestamp" and "pid". (Alternately,
if your format is not supported yet, feel free to send a PR to add support
for it!)

For the logs to include the data that is useful to visualize, it is
also assumed that `log_lock_waits` is on and that `log_min_duration_statement`
is set to a threshold that logs any queries that have an interesting
duration. `log_connections` and `log_disconnections` will provide
additional information.

Adding additional information in the log prefix line can make it easier
to debug issues. In particular: if your application can set the
"application name" to include as much information as possible about the
context in which the query was made, and `log_line_prefix` can be
configured to include `%a`, this can be extremely helpful, as it allows
you to easily correlate the Postgres logs with your application's logs.

Two "carrier" input formats are currently supported:

- Plain text.
- JSON as exported from Google Cloud Logging (e.g. for a Postgres database
  running on Google Compute Engine).

## Navigating the visualization

The generated visualization is a HTML file which can be opened in a browser.

The X-axis is time.

The Y-axis is Postgres processes.

Rectangles represent events that covered spans of time on the database. Chiefly,
this means slow SQL statements. (Note that which statements are included here
depends on your `log_min_duration_statement` setting.) Similar SQL statements
are coloured similarly.

Circles represent "instant" events, coloured by event type.

You can hover over elements of the visualization to see more information
displayed in the information panel on the left side of the screen.
Click the element to "lock" in the highlighted element. (Click again to unlock.)

Pay particular attention to the black circles, which represent "waiting for
lock" events. If a statement was blocked waiting for a lock, an event like
this should be generated shortly after the start of the statement.
If you highlight these events, the process that _held_ the lock, as well
as the other processes in the wait queue, will also be highlighted.
