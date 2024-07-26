import click
import orjson
from anystore.io import smart_write
from anystore.util import clean_dict
from click_default_group import DefaultGroup

from ftmq.aggregate import aggregate
from ftmq.io import apply_datasets, smart_read_proxies, smart_write_proxies
from ftmq.logging import configure_logging, get_logger
from ftmq.model.coverage import Collector
from ftmq.model.dataset import Catalog, Dataset
from ftmq.query import Query
from ftmq.store import get_store
from ftmq.util import parse_unknown_filters

log = get_logger(__name__)


@click.group(cls=DefaultGroup, default="q", default_if_no_args=True)
def cli() -> None:
    configure_logging()


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
    "--stats-uri",
    default=None,
    show_default=True,
    help="If specified, print statistic coverage information to this uri",
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
    "--count", multiple=True, help="Properties for count (distinct) aggregation"
)
@click.option("--groups", multiple=True, help="Properties for grouping aggregation")
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
    stats_uri: str | None = None,
    store_dataset: str | None = None,
    sum: tuple[str] | None = (),
    min: tuple[str] | None = (),
    max: tuple[str] | None = (),
    avg: tuple[str] | None = (),
    count: tuple[str] | None = (),
    groups: tuple[str] | None = (),
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
        q = q.where(**{f"{prop}__{op}": value})
    if len(sort):
        q = q.order_by(*sort, ascending=sort_ascending)

    if len(dataset) == 1:
        store_dataset = store_dataset or dataset[0]
    aggs = {
        k: v
        for k, v in {
            "sum": sum,
            "min": min,
            "max": max,
            "avg": avg,
            "count": count,
        }.items()
        if v
    }
    if aggregation_uri and aggs:
        for func, props in aggs.items():
            q = q.aggregate(func, *props, groups=groups)
    proxies = smart_read_proxies(input_uri, dataset=store_dataset, query=q)
    if stats_uri:
        stats = Collector()
        proxies = stats.apply(proxies)
    smart_write_proxies(output_uri, proxies, serialize=True, dataset=store_dataset)
    if stats_uri:
        stats = stats.export()
        stats = orjson.dumps(stats.model_dump(), option=orjson.OPT_APPEND_NEWLINE)
        smart_write(stats_uri, stats)
    if q.aggregator:
        result = orjson.dumps(
            clean_dict(q.aggregator.result), option=orjson.OPT_APPEND_NEWLINE
        )
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


@cli.group()
def dataset():
    pass


@dataset.command("iterate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def dataset_iterate(input_uri: str | None = "-", output_uri: str | None = "-"):
    dataset = Dataset._from_uri(input_uri)
    smart_write_proxies(output_uri, dataset.iterate(), serialize=True)


@dataset.command("generate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option(
    "--stats",
    is_flag=True,
    default=False,
    show_default=True,
    help="Calculate stats",
)
def make_dataset(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    stats: bool | None = False,
):
    """
    Convert dataset YAML specification into json and optionally calculate statistics
    """
    dataset = Dataset._from_uri(input_uri)
    if stats:
        collector = Collector()
        statistics = collector.collect_many(dataset.iterate())
        dataset.apply_stats(statistics)
    smart_write(output_uri, dataset.model_dump_json().encode())


@cli.group()
def catalog():
    pass


@catalog.command("iterate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def catalog_iterate(input_uri: str | None = "-", output_uri: str | None = "-"):
    catalog = Catalog._from_uri(input_uri)
    smart_write_proxies(output_uri, catalog.iterate(), serialize=True)


@catalog.command("generate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option(
    "--stats",
    is_flag=True,
    default=False,
    show_default=True,
    help="Calculate stats for each dataset",
)
def make_catalog(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    stats: bool | None = False,
):
    """
    Convert catalog YAML specification into json and fetch dataset metadata
    """
    catalog = Catalog._from_uri(input_uri)
    if stats:
        for dataset in catalog.datasets:
            log.info(f"Generating stats for `{dataset.name}` ...")
            collector = Collector()
            statistics = collector.collect_many(dataset.iterate())
            dataset.apply_stats(statistics)
    smart_write(output_uri, catalog.model_dump_json().encode())


@cli.group()
def store():
    pass


@store.command("list-datasets")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
def store_list_datasets(
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


@store.command("resolve")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="store input uri"
)
@click.option(
    "-o", "--output-uri", default=None, show_default=True, help="output file or uri"
)
@click.option(
    "-r",
    "--resolver-uri",
    default=None,
    show_default=True,
    help="resolver uri",
    required=True,
)
def store_resolve(
    input_uri: str | None = "-",
    output_uri: str | None = None,
    resolver_uri: str | None = None,
):
    """
    Apply nk resolver to a store
    """
    store = get_store(input_uri, resolver=resolver_uri)
    store.resolve()
    if output_uri:
        smart_write_proxies(output_uri, store.iterate(), serialize=True)


@store.command("iterate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="store input uri"
)
@click.option(
    "-o", "--output-uri", default=None, show_default=True, help="output file or uri"
)
def store_iterate(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
):
    """
    Iterate all entities from in to out
    """
    store = get_store(input_uri)
    smart_write_proxies(output_uri, store.iterate(), serialize=True)


@cli.command("aggregate")
@click.option(
    "-i", "--input-uri", default="-", show_default=True, help="input file or uri"
)
@click.option(
    "-o", "--output-uri", default="-", show_default=True, help="output file or uri"
)
@click.option("--downgrade", is_flag=True, default=False, show_default=True)
def cli_aggregate(
    input_uri: str | None = "-",
    output_uri: str | None = "-",
    downgrade: bool | None = False,
):
    """
    In-memory aggregation of entities, allowing to merge entities with a common
    parent schema (as opposed to standard `ftm aggregate`)
    """
    proxies = aggregate(smart_read_proxies(input_uri), downgrade=downgrade)
    smart_write_proxies(output_uri, proxies, serialize=True)
