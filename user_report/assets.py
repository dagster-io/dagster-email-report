from geopy.distance import geodesic
import base64
import pandas as pd
import numpy as np
from .resources import LocalFileStorage, Database, EmailService
from . import charts

from dagster import (
    AssetExecutionContext,
    asset,
    op,
    job,
    AssetIn,
    TimeWindowPartitionMapping,
    MonthlyPartitionsDefinition,
)

monthly_partition_def = MonthlyPartitionsDefinition(start_date="2023-03-01")


@asset(
    partitions_def=monthly_partition_def,
    metadata={"partition_expr": "month_end"},
)
def monthly_reservations(
    context: AssetExecutionContext, database: Database
) -> pd.DataFrame:
    bounds = context.partition_time_window
    results = database.query(
        f"""
        SELECT
            r.id,
            r.property_id,
            r.guest_id,
            r.created_at,
            r.total_cost,
            g.lat AS lat_guest,
            g.lon AS lon_guest,
            p.lon,
            p.lat,
            p.host_id,
            p.market_name
        FROM
            reservation r
        LEFT JOIN
            property p ON r.property_id = p.id
        LEFT JOIN
            guest g ON r.guest_id = g.id
        WHERE r.end_date >= '{bounds.start}' AND r.end_date < '{bounds.end}'
    """
    )

    results["dist"] = results.apply(calc_dist, axis=1)
    results["month_end"] = pd.to_datetime(bounds.end)
    return results


def calc_dist(row):
    return geodesic(
        (row["lat"], row["lon"]), (row["lat_guest"], row["lon_guest"])
    ).miles


@asset(
    partitions_def=monthly_partition_def,
    metadata={"partition_expr": "month_end"},
)
def property_analytics(
    monthly_reservations: pd.DataFrame,
) -> pd.DataFrame:
    reservations_grouped = (
        monthly_reservations.groupby(
            ["property_id", "month_end", "market_name", "host_id"]
        )
        .agg(
            total_revenue=pd.NamedAgg(column="total_cost", aggfunc="sum"),
            num_local_reservations=pd.NamedAgg(
                column="dist", aggfunc=lambda x: (x <= 100).sum()
            ),
        )
        .reset_index()
    )

    return reservations_grouped[
        [
            "property_id",
            "host_id",
            "market_name",
            "month_end",
            "total_revenue",
            "num_local_reservations",
        ]
    ]


# @asset(
#     partitions_def=monthly_partition_def,
#     metadata={"partition_expr": "month_end"},
# )
# def market_analytics(
#     property_analytics: pd.DataFrame,
# ) -> pd.DataFrame:
#     def create_histogram(data, bins=10):
#         hist, bin_edges = np.histogram(data, bins=bins)
#         return {"bins_start": bin_edges[:-1], "bins_end": bin_edges[1:], "counts": hist}

#     market_df = property_analytics.groupby(["market_name", "month_end"]).agg(
#         total_properties=pd.NamedAgg(column="property_id", aggfunc="count"),
#         total_revenue_hist=pd.NamedAgg(
#             column="total_revenue", aggfunc=lambda x: create_histogram(x)
#         ),
#     )

#     market_df.reset_index(inplace=True)

#     return market_df


@asset(
    partitions_def=monthly_partition_def,
    ins={
        "property_analytics": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-4),
        )
    },
    metadata={"partition_expr": "month_end"},
)
def historical_bar_charts(
    property_analytics: pd.DataFrame,
    image_storage: LocalFileStorage,
) -> pd.DataFrame:
    chart_paths = []
    for property_id in property_analytics["property_id"].unique():
        property_data = property_analytics[
            property_analytics["property_id"] == property_id
        ].sort_values(by="month_end")
        chart_buffer = charts.draw_line_chart(property_data, "total_revenue")
        last_month_end = property_data.iloc[-1]["month_end"]
        last_month_end_str = last_month_end.strftime("%Y/%m")

        chart_path = f"{last_month_end_str}/total_revenue_property_{property_id}.png"
        image_storage.write(
            chart_path,
            chart_buffer,
        )
        chart_paths.append(
            {
                "property_id": property_id,
                "total_revenue_chart": chart_path,
            }
        )

    line_charts = pd.DataFrame(chart_paths)
    line_charts["month_end"] = property_analytics["month_end"].max()

    return line_charts


@asset(
    partitions_def=monthly_partition_def,
    metadata={"partition_expr": "month_end"},
)
def emails_to_send(
    property_analytics: pd.DataFrame,
    historical_bar_charts: pd.DataFrame,
    database: Database,
) -> pd.DataFrame:
    hosts = database.query("SELECT * FROM host")
    df_merged = property_analytics.merge(
        hosts,
        left_on="host_id",
        right_on="id",
        how="left",
        suffixes=("", "_host"),
    ).merge(
        historical_bar_charts,
        left_on=["property_id", "month_end"],
        right_on=["property_id", "month_end"],
        how="left",
    )

    return df_merged


@op
def send_emails(
    context: AssetExecutionContext,
    emails: pd.DataFrame,
    email_service: EmailService,
    image_storage: LocalFileStorage,
) -> None:
    bounds = context.partition_time_window
    filtered_df = emails[
        (emails["month_end"] > bounds.start) & (emails["month_end"] <= bounds.end)
    ]
    for _, row in filtered_df.iterrows():
        encoded_string = base64.b64encode(
            image_storage.read(row["total_revenue_chart"])
        ).decode()
        email_service.send(
            row["email"],
            template={"name": row["name"], "revenue": row["total_revenue"]},
            attachments=[
                {
                    "Name": row["total_revenue_chart"],
                    "Content": encoded_string,
                    "ContentType": "image/png",
                    "ContentID": f"cid:{row['total_revenue_chart']}",
                }
            ],
        )


@job(
    partitions_def=monthly_partition_def,
)
def send_emails_job():
    send_emails(emails_to_send.to_source_asset())
