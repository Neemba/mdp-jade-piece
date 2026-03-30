import dagster as dg
from .jobs import daily_job, full_refresh_job


@dg.schedule(
    name="daily_schedule",
    cron_schedule="15 1 * * *",  # 1:15 AM daily
    job=daily_job,
    description="Daily incremental load with smart extraction (auto JDBC/UNLOAD/UNLOAD_DIRECT)",
)
def daily_schedule():
    """Daily schedule for incremental loads."""
    return dg.RunRequest()


@dg.schedule(
    name="weekly_full_refresh",
    # cron_schedule="0 1 * * 0",  # 1 AM every Sunday
    cron_schedule="15 1 * * *",  # 1:15 AM daily
    job=full_refresh_job,
    description="Weekly full refresh with smart extraction",
)
def weekly_full_refresh():
    """Weekly full refresh schedule."""
    return dg.RunRequest()
