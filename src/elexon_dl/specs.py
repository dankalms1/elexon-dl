from __future__ import annotations
from typing import Dict
from .engine import EndpointSpec, TimeStrategy
from .filters import exact_sp_filter, within_dayahead_window, enrich_publish_effective, wind_evolution_top8

SPEC_REGISTRY: Dict[str, EndpointSpec] = {}

SPEC_REGISTRY["isp_stack"] = EndpointSpec(
    name="isp_stack",
    path_template="/balancing/settlement/stack/all/{bidOfferType}/{date}/{sp}",
    query_template={},
    dims={"bidOfferType": ["bid","offer"]},
    time=TimeStrategy(kind="date_sp"),
    items_path="data",
    table="isp_stack",
    primary_keys=("settlementDate","settlementPeriod","bidOfferType"),
)

SPEC_REGISTRY["bidoffer_price_acceptances"] = EndpointSpec(
    name="bidoffer_price_acceptances",
    path_template="/balancing/settlement/acceptances/all/{date}/{sp}",
    query_template={},
    time=TimeStrategy(kind="date_sp"),
    items_path="data",
    table="bidoffer_price_acceptances",
    primary_keys=("settlementDate","settlementPeriod","bmUnitId","acceptanceId"),
    row_filter=exact_sp_filter,
)

SPEC_REGISTRY["bidoffer_level_acceptances"] = EndpointSpec(
    name="bidoffer_level_acceptances",
    path_template="/balancing/acceptances/all",
    query_template={"settlementDate": "{date}", "settlementPeriod": "{sp}"},
    time=TimeStrategy(kind="date_sp"),
    items_path="data",
    table="bidoffer_level_acceptances",
    primary_keys=("settlementDate","settlementPeriod","bmUnitId","acceptanceId"),
    row_filter=exact_sp_filter,
)

SPEC_REGISTRY["dayahead_demand_history"] = EndpointSpec(
    name="dayahead_demand_history",
    path_template="/forecast/demand/day-ahead/history",
    query_template={"publishTime":"{publishTime}"},
    time=TimeStrategy(kind="halfhour_slots"),
    items_path="data",
    table="dayahead_demand_history",
    primary_keys=("publishTimeEffective","startTime","region"),
    enricher=enrich_publish_effective,
    row_filter=within_dayahead_window,
)

SPEC_REGISTRY["wind_history"] = EndpointSpec(
    name="wind_history",
    path_template="/forecast/generation/wind/history",
    query_template={"publishTime": "{publishTime}"},
    time=TimeStrategy(
        kind="publish_slots_fixed_utc",
        publish_slots=["03:30","05:30","08:30","10:30","12:30","16:30","19:30","23:30"],  # UTC
    ),
    items_path="data",
    table="wind_history",
    primary_keys=("publishTime","startTime"),
    enricher=enrich_publish_effective,     # stamps effective publish time
    row_filter=within_dayahead_window,     # keep rows in [publish+30m, publish+24h]
)

SPEC_REGISTRY["wind_evolution"] = EndpointSpec(
    name="wind_evolution",
    path_template="/forecast/generation/wind/evolution",
    query_template={"startTime":"{publishTime}", "format":"json"},
    time=TimeStrategy(kind="halfhour_slots"),
    items_path="data",
    table="wind_evolution",
    primary_keys=("startTime","publishTime"),
    row_filter=wind_evolution_top8,
)

SPEC_REGISTRY["agpt"] = EndpointSpec(
    name="agpt",
    path_template="/datasets/AGPT",
    query_template={"publishDateTimeFrom":"{from_ts}","publishDateTimeTo":"{to_ts}"},
    time=TimeStrategy(kind="from_to"),
    items_path="data",
    table="agpt",
    primary_keys=("publishTime","bmUnitId","startTime"),
)

SPEC_REGISTRY["agws"] = EndpointSpec(
    name="agws",
    path_template="/datasets/AGWS",
    query_template={"publishDateTimeFrom":"{from_ts}","publishDateTimeTo":"{to_ts}"},
    time=TimeStrategy(kind="from_to"),
    items_path="data",
    table="agws",
    primary_keys=("publishTime","startTime"),
)

SPEC_REGISTRY["system_prices"] = EndpointSpec(
    name="system_prices",
    path_template="/balancing/settlement/system-prices/{date}",
    time=TimeStrategy(kind="date_only"),
    items_path="data",
    table="system_prices",
    primary_keys=("settlementDate","settlementPeriod","priceType"),
)

SPEC_REGISTRY["demand_outturn"] = EndpointSpec(
    name="demand_outturn",
    path_template="/demand/outturn",
    query_template={"settlementDateFrom":"{date}","settlementDateTo":"{date}"},
    time=TimeStrategy(kind="date_only"),
    items_path="data",
    table="demand_outturn",
    primary_keys=("settlementDate","settlementPeriod"),
)

SPEC_REGISTRY["netbsad"] = EndpointSpec(
    name="netbsad",
    path_template="/datasets/netbsad",
    query_template={"from":"{from_ts}","to":"{to_ts}"},
    time=TimeStrategy(kind="from_to"),
    items_path="data",
    table="netbsad",
    primary_keys=("publishTime","recordId"),
)
