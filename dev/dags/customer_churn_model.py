"""Example DAG for trying the plugin out; the tasks do nothing."""

from datetime import UTC, datetime

from airflow.sdk import dag, task

DOC = """
# Customer churn model

Retrains the model that predicts which customers are likely to cancel within the next 90 days,
every Monday on the previous twelve months of activity.

A new model is only published when it beats the current one on the holdout set; otherwise the run
succeeds and the old model stays in place. Scores feed the retention campaign in the CRM.
"""


@dag(
    dag_id="customer_churn_model",
    dag_display_name="Customer churn model",
    schedule="0 3 * * 1",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["ml"],
    doc_md=DOC,
)
def customer_churn_model():
    @task(doc_md="Aggregates logins, orders and support tickets per customer and week.")
    def build_features():
        pass

    @task
    def train_model():
        pass

    @task(doc_md="Compares the new model's AUC on the holdout set with the published one.")
    def evaluate_model():
        pass

    @task(doc_md="Registers the model and writes this week's scores for the CRM.")
    def publish_model():
        pass

    build_features() >> train_model() >> evaluate_model() >> publish_model()


customer_churn_model()
