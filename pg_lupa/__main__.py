import sys

import click

from . import lupa


@click.command()
@click.option("--input-logs", type=click.File("r"))
@click.option("--output-html", type=click.File("w"))
@click.option("--sort-processes-by", default="time")
def main(input_logs, output_html, sort_processes_by):
    options = lupa.VizOptions(
        process_sort_order=lupa.ProcessSortOrder(sort_processes_by),
    )

    model = lupa.parse_log_data_automagically(input_logs or sys.stdin)

    lupa.visualize(model, output_html or sys.stdout, options)


if __name__ == "__main__":
    main()
