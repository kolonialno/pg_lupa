import sys

import click

from . import lupa


@click.command()
@click.option("--input-logs", type=click.File("r"))
@click.option("--output-html", type=click.File("w"))
@click.option("--sort-processes-by", default="time")
@click.option("--log-line-prefix-format", default=None, type=str)
@click.option("--log-line-prefix-regex", default=None, type=str)
def main(
    input_logs,
    output_html,
    sort_processes_by,
    log_line_prefix_format,
    log_line_prefix_regex,
):
    viz_options = lupa.VizOptions(
        process_sort_order=lupa.ProcessSortOrder(sort_processes_by),
    )
    parse_options = lupa.ParseOptions(
        log_line_prefix_format=log_line_prefix_format,
        log_line_prefix_regex=log_line_prefix_regex,
    )

    model = lupa.parse_log_data_automagically(input_logs or sys.stdin, parse_options)

    lupa.visualize(model, output_html or sys.stdout, viz_options)


if __name__ == "__main__":
    main()
