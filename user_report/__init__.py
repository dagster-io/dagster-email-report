from dagster import (
    Definitions,
    load_assets_from_modules,
    ScheduleDefinition,
)

from dagster_duckdb_pandas import DuckDBPandasIOManager

from . import assets
from .resources import LocalFileStorage, Database, EmailService
from .assets import send_emails_job

all_assets = load_assets_from_modules([assets])
database_io_manager = DuckDBPandasIOManager(database="myvacation.duckdb", schema="main")

send_emails_schedule = ScheduleDefinition(
    job=send_emails_job,
    cron_schedule="0 0 1 * *",  # every month
)

defs = Definitions(
    assets=all_assets,
    jobs=[send_emails_job],
    schedules=[send_emails_schedule],
    resources={
        "io_manager": database_io_manager,
        "image_storage": LocalFileStorage(dir="charts"),
        "database": Database(path="myvacation.duckdb"),
        "email_service": EmailService(
            template_id=123,  # EnvVar("EMAIL_TEMPLATE_ID"),
            sender_email="sender_email",  # EnvVar("EMAIL_SENDER_EMAIL"),
            server_token="server_token",  # EnvVar("EMAIL_SERVER_TOKEN"),
        ),
    },
)
