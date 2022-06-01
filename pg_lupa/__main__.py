import sys

import click

from . import lupa


@click.command()
@click.option(
    "--input-logs", type=click.File("r"), help="File from which to read Postgres logs"
)
@click.option(
    "--output-html", type=click.File("w"), help="File to which to write a report (HTML)"
)
@click.option(
    "--timezone",
    default=None,
    help="Time zone to be used in output report (e.g. 'Europe/Oslo')",
)
@click.option(
    "--sort-processes-by",
    default="time",
    type=click.Choice(list(lupa.ProcessSortOrder.__members__), case_sensitive=False),
    help="Sort order for processes in report",
)
@click.option(
    "--log-line-prefix-format",
    default=None,
    type=str,
    help="Postgres log_line_prefix setting (not all formatting is supported)",
)
@click.option(
    "--log-line-prefix-regex",
    default=None,
    type=str,
    help="Regex with capturing groups to match log line prefixes",
)
def main(
    input_logs,
    output_html,
    sort_processes_by,
    log_line_prefix_format,
    log_line_prefix_regex,
    timezone,
):
    viz_options = lupa.VizOptions(
        process_sort_order=lupa.ProcessSortOrder(sort_processes_by.lower()),
        timezone=timezone,
    )
    parse_options = lupa.ParseOptions(
        log_line_prefix_format=log_line_prefix_format,
        log_line_prefix_regex=log_line_prefix_regex,
    )

    lupa.run_analyzer(
        input_file=input_logs or sys.stdin,
        output_file=output_html or sys.stdout,
        parse_options=parse_options,
        viz_options=viz_options,
    )


if __name__ == "__main__":
    main()
