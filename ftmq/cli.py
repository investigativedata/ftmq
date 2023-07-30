import click
import orjson
from click_default_group import DefaultGroup

from ftmq.io import (
    apply_datasets,
    smart_read,
    smart_read_proxies,
    smart_write,
    smart_write_proxies,
)

from .model.coverage import Collector
from .query import Query
from .util import parse_unknown_cli_filters


@click.group(cls=DefaultGroup, default="q", default_if_no_args=True)
def cli():
    pass


@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
    ),
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
@click.option(
    "--coverage-uri",
    default=None,
    help="If specified, print coverage information to this uri",
)
@click.argument("properties", nargs=-1)
def q(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    dataset: tuple[str] | None = (),
    schema: tuple[str] | None = (),
    schema_include_descendants: bool | None = False,
    schema_include_matchable: bool | None = False,
    properties: tuple[str] | None = (),
    coverage_uri: str | None = None,
):
    """
    Apply ftmq filter to a json stream of ftm entities.
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
    if coverage_uri:
        coverage = Collector.apply(proxies)
        coverage = orjson.dumps(coverage.dict(), option=orjson.OPT_APPEND_NEWLINE)
        smart_write(coverage_uri, coverage)
    smart_write_proxies(output_uri, proxies, serialize=True)


@cli.command("apply")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("-d", "--dataset", multiple=True, help="Dataset(s) to filter for")
@click.option("--replace-dataset", is_flag=True, default=False, show_default=True)
def apply(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    dataset: tuple[str] | None = (),
    replace_dataset: bool | None = False,
):
    """
    Uplevel an entity stream to nomenklatura entities and apply dataset(s) property
    """

    proxies = smart_read_proxies(input_uri)
    if dataset:
        proxies = apply_datasets(proxies, *dataset, replace=replace_dataset)
    smart_write_proxies(output_uri, proxies, serialize=True)


@cli.command("io")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def io(input_uri: str | None = "-", output_uri: str | None = "-"):
    """
    Generic cli wrapper around ftmq.io.smart_open
    """
    smart_write(output_uri, smart_read(input_uri))
