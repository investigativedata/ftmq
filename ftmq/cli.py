from typing import Optional

import typer

from .query import Query
from .util import read_proxies, write_proxy

cli = typer.Typer()


@cli.command()
def ftmq(
    input_file: typer.FileBinaryRead = typer.Option("-", "-i", help="input file"),
    output_file: typer.FileBinaryWrite = typer.Option("-", "-o", help="output file"),
    dataset: Optional[list[str]] = typer.Option(
        None, "-d", "--dataset", help="Dataset(s)"
    ),
    schema: Optional[list[str]] = typer.Option(
        None, "-s", "--schema", help="Schema(tas)"
    ),
    schema_include_descendants: bool = False,
    schema_include_matchable: bool = False,
):
    """
    Apply ftmq to a json stream of ftm entities.
    """
    q = Query()
    for value in dataset:
        q = q.where(dataset=value)
    for value in schema:
        q = q.where(
            schema=value,
            include_descendants=schema_include_descendants,
            include_matchable=schema_include_matchable,
        )

    for proxy in read_proxies(input_file):
        if q.apply(proxy):
            write_proxy(output_file, proxy)
