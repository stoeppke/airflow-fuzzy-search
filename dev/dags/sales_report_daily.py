"""Example DAG for trying the plugin out; the tasks do nothing."""

from datetime import UTC, datetime

from airflow.sdk import dag, task

DOC = """
# Daily sales report

Builds the sales report the finance team reads every morning: yesterday's orders per region and
product line, converted to euros with the exchange rates of the order day.

Refunds are booked against the day of the original order, so earlier days can change. The report
is sent by mail at 07:00 and archived as a PDF next to the invoices.
"""


@dag(
    dag_id="sales_report_daily",
    dag_display_name="Daily sales report",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["reporting", "finance"],
    doc_md=DOC,
)
def sales_report_daily():
    @task(doc_md="Reads yesterday's orders and refunds from the shop database replica.")
    def extract_orders():
        pass

    @task(doc_md="Converts every amount to euros with the exchange rate of the order day.")
    def convert_currencies():
        pass

    @task
    def build_report():
        pass

    @task(doc_md="Mails the report to the finance distribution list.")
    def send_report():
        pass

    extract_orders() >> convert_currencies() >> build_report() >> send_report()


sales_report_daily()
