"""Example DAG for trying the plugin out; the tasks do nothing."""

from datetime import UTC, datetime

from airflow.sdk import dag, task

DOC = """
# Weather ingest

Pulls the latest **weather observations** for every station in the network from the public
weather service once an hour and lands them unchanged in the raw zone of the lake.

Late observations are picked up by the next run, because each run re-reads the last three hours.
Ask the data platform team before changing the station list.
"""


@dag(
    dag_id="weather_ingest",
    dag_display_name="Weather ingest",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["ingest", "api"],
    doc_md=DOC,
)
def weather_ingest():
    @task(doc_md="Calls the weather service's observations endpoint, one request per station batch.")
    def fetch_observations():
        pass

    @task(doc_md="Rejects observations without a station id or with a timestamp in the future.")
    def validate_schema():
        pass

    @task(doc_md="Appends the batch to `raw.weather_observations`, partitioned by day.")
    def load_raw():
        pass

    fetch_observations() >> validate_schema() >> load_raw()


weather_ingest()
