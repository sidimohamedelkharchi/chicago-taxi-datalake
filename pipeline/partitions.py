from dagster import MonthlyPartitionsDefinition

from pipeline.config import settings

# One partition = one month of trips (filtered on trip_start_timestamp).
# end_date is exclusive: 2023-01 -> 2023-04 gives Jan, Feb, Mar 2023 (Q1).
monthly_partitions = MonthlyPartitionsDefinition(
    start_date=f"{settings.start_month}-01",
    end_date=f"{settings.end_month}-01",
    fmt="%Y-%m-%d",
)


def partition_to_month(partition_key: str) -> str:
    """'2023-01-01' -> '2023-01'."""
    return partition_key[:7]
