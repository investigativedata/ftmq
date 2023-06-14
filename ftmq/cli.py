from typing import Optional

import typer

from .query import Query
from .util import smart_read_proxies, smart_write_proxies

cli = typer.Typer()


@cli.command()
def ftmq(
    input_uri: str = typer.Option("-", "-i", help="input file or uri"),
    output_uri: str = typer.Option("-", "-o", help="output file or uri"),
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

    proxies = q.apply_iter(smart_read_proxies(input_uri))
    smart_write_proxies(proxies)
