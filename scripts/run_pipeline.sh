#!/usr/bin/env bash
# End-to-end run from the CLI (same Dagster instance as the UI -> runs are visible
# at http://localhost:3000/runs). Executed inside the dagster-daemon container.
set -euo pipefail

DEFS="-m pipeline.definitions"

echo "==> [1/3] Reference data (community areas): bronze -> silver"
dagster asset materialize $DEFS --select "bronze_community_areas,silver_community_areas"

MONTHS=$(python -c "from pipeline.partitions import monthly_partitions as p; print(' '.join(p.get_partition_keys()))")
for partition in $MONTHS; do
  echo "==> [2/3] Trips ${partition:0:7}: SODA API -> bronze -> silver"
  dagster asset materialize $DEFS --select "bronze_taxi_trips,silver_taxi_trips" --partition "$partition"
done

echo "==> [3/3] Gold tables"
dagster asset materialize $DEFS --select "group:gold"

echo "Done. Dagster: http://localhost:3000  |  Dashboard: http://localhost:8501  |  MinIO: http://localhost:9001"
