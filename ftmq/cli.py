from typing import Optional

import typer
from typing_extensions import Annotated

from ftmq.io import smart_read_proxies, smart_write_proxies

from .query import Query

cli = typer.Typer()


@cli.command()
def ftmq(
    input_uri: Annotated[
        Optional[str], typer.Option("-i", help="input file or uri")
    ] = "-",
    output_uri: Annotated[
        Optional[str], typer.Option("-o", help="output file or uri")
    ] = "-",
    dataset: Annotated[
        Optional[list[str]], typer.Option("-d", "--dataset", help="Dataset(s)")
    ] = None,
    schema: Annotated[
        Optional[list[str]], typer.Option("-s", "--schema", help="Schema(tas)")
    ] = None,
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
    smart_write_proxies(output_uri, proxies, serialize=True)
