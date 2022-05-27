import sys

import click

from . import lupa


@click.command()
@click.option("--input-logs", type=click.File("r"))
@click.option("--output-html", type=click.File("w"))
def main(input_logs, output_html):
    model = lupa.parse_log_data_automagically(input_logs or sys.stdin)
    lupa.visualize(model, output_html or sys.stdout)


if __name__ == "__main__":
    main()
