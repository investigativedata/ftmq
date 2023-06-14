import click

from ftmq.io import smart_read_proxies, smart_write_proxies

from .query import Query
from .util import parse_unknown_cli_filters


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
    )
)
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("-d", "--dataset", multiple=True, help="Dataset(s) to filter for")
@click.option("-s", "--schema", multiple=True, help="Schema(s) to filter for")
@click.option(
    "--schema-include-descendants", is_flag=True, default=False, show_default=True
)
@click.option(
    "--schema-include-matchable", is_flag=True, default=False, show_default=True
)
@click.argument("properties", nargs=-1)
def cli(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    dataset: tuple[str] | None = (),
    schema: tuple[str] | None = (),
    schema_include_descendants: bool | None = False,
    schema_include_matchable: bool | None = False,
    properties: tuple[str] | None = (),
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
    for prop, value, op in parse_unknown_cli_filters(properties):
        q = q.where(prop=prop, value=value, operator=op)

    proxies = q.apply_iter(smart_read_proxies(input_uri))
    smart_write_proxies(output_uri, proxies, serialize=True)
