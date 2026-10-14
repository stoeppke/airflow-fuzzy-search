"""Example DAG for trying the plugin out; the tasks do nothing."""

from datetime import UTC, datetime

from airflow.sdk import dag, task

DOC = """
# Warehouse maintenance

Keeps the lake's tables fast and small: compacts the small files that streaming writes leave
behind, expires snapshots older than seven days and removes files no snapshot references.

Runs at night when nothing else writes. If a compaction fails on a lock, the next night retries it.
"""


@dag(
    dag_id="warehouse_maintenance",
    dag_display_name="Warehouse maintenance",
    schedule="30 1 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["ops"],
    doc_md=DOC,
)
def warehouse_maintenance():
    @task(doc_md="Rewrites partitions with more than fifty small files into files of about 256 MB.")
    def compact_tables():
        pass

    @task(doc_md="Drops snapshots older than seven days, keeping at least the last five.")
    def expire_snapshots():
        pass

    @task
    def remove_orphan_files():
        pass

    compact_tables() >> expire_snapshots() >> remove_orphan_files()


warehouse_maintenance()
