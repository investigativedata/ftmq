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
from ftmq.model.coverage import Collector
from ftmq.query import Query
from ftmq.store import get_store
from ftmq.util import parse_unknown_filters


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
@click.option("--sort", help="Properties to sort for", multiple=True)
@click.option(
    "--sort-ascending/--sort-descending",
    is_flag=True,
    help="Sort in ascending order",
    default=True,
    show_default=True,
)
@click.option(
    "--coverage-uri",
    default=None,
    show_default=True,
    help="If specified, print coverage information to this uri",
)
@click.option(
    "--store-dataset",
    default=None,
    show_default=True,
    help="If specified, default dataset for source and target stores",
)
@click.option("--sum", multiple=True, help="Properties for sum aggregation")
@click.option("--min", multiple=True, help="Properties for min aggregation")
@click.option("--max", multiple=True, help="Properties for max aggregation")
@click.option("--avg", multiple=True, help="Properties for avg aggregation")
@click.option(
    "--aggregation-uri",
    default=None,
    show_default=True,
    help="If specified, print aggregation information to this uri",
)
@click.argument("properties", nargs=-1)
def q(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    dataset: tuple[str] | None = (),
    schema: tuple[str] | None = (),
    schema_include_descendants: bool | None = False,
    schema_include_matchable: bool | None = False,
    sort: tuple[str] | None = None,
    sort_ascending: bool | None = True,
    properties: tuple[str] | None = (),
    coverage_uri: str | None = None,
    store_dataset: str | None = None,
    sum: tuple[str] | None = (),
    min: tuple[str] | None = (),
    max: tuple[str] | None = (),
    avg: tuple[str] | None = (),
    aggregation_uri: str | None = None,
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
    for prop, value, op in parse_unknown_filters(properties):
        q = q.where(prop=prop, value=value, comparator=op)
    if len(sort):
        q = q.order_by(*sort, ascending=sort_ascending)

    if len(dataset) == 1:
        store_dataset = store_dataset or dataset[0]
    aggs = {
        k: v for k, v in {"sum": sum, "min": min, "max": max, "avg": avg}.items() if v
    }
    if aggregation_uri and aggs:
        for func, props in aggs.items():
            q = q.aggregate(func, *props)
    proxies = smart_read_proxies(input_uri, dataset=store_dataset, query=q)
    if coverage_uri:
        coverage = Collector()
        proxies = coverage.apply(proxies)
    smart_write_proxies(output_uri, proxies, serialize=True, dataset=store_dataset)
    if coverage_uri:
        coverage = coverage.export()
        coverage = orjson.dumps(coverage.dict(), option=orjson.OPT_APPEND_NEWLINE)
        smart_write(coverage_uri, coverage)
    if q.aggregator:
        result = orjson.dumps(q.aggregator.result, option=orjson.OPT_APPEND_NEWLINE)
        smart_write(aggregation_uri, result)


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


@cli.command("list-datasets")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def list_datasets(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
):
    """
    List datasets within a store
    """
    store = get_store(input_uri)
    catalog = store.get_catalog()
    datasets = [ds.name for ds in catalog.datasets]
    smart_write(output_uri, "\n".join(datasets).encode() + b"\n")


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
