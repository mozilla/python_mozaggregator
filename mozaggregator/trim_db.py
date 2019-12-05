import click
import psycopg2

from typing import Set, List, Tuple
from datetime import datetime, timedelta

DS_NODASH = "%Y%m%d"


def extract_ds_nodash(tablename):
    return tablename.split("_")[-1]


def retention_date_range(base: str, period: int = 365, buffer: int = 7) -> Set[str]:
    """Create a set of dates between [base-period, base]. The date format is ds_nodash."""
    base = datetime.strptime(base, DS_NODASH)
    num_days = period + buffer
    dates = set(
        [
            datetime.strftime(base - timedelta(period) + timedelta(x), DS_NODASH)
            for x in range(num_days)
        ]
    )
    return dates


def create_connection(dbname, user, password, host):
    conn_str = f"dbname={dbname} user={user} password={password} host={host}"
    conn = psycopg2.connect(conn_str)
    return conn


def display_summary(action: str, table_set: Set[str], tables_to_show: int = 10):
    tables = list(sorted(table_set, key=extract_ds_nodash))
    print(f"To {action} {len(tables)} tables...")
    print("-" * 40)
    if len(tables) > tables_to_show:
        show = tables[: tables_to_show // 2] + ["..."] + tables[-tables_to_show // 2 :]
    else:
        show = tables
    print("\n".join(show))
    print("=" * 40)


def partition_set_by_filter(
    full_set: Set[str], retain_suffix_set: Set[str]
) -> Tuple[Set[str], Set[str]]:
    retain_set = {
        table for table in full_set if extract_ds_nodash(table) in retain_suffix_set
    }
    trim_set = full_set - retain_set
    return retain_set, trim_set


def query_submission_date(
    cursor, retain_suffix_set: Set[str]
) -> Tuple[Set[str], Set[str]]:
    submission_date_query = """
    select tablename
    from pg_catalog.pg_tables
    where schemaname='public' and tablename like 'submission_date%';
    """
    cursor.execute(submission_date_query)
    submission_retain, submission_trim = partition_set_by_filter(
        {row[0] for row in cursor.fetchall()}, retain_suffix_set
    )
    display_summary("retain", submission_retain)
    display_summary("trim", submission_trim)
    return submission_retain, submission_trim


def query_build_id(cursor, retain_suffix_set: Set[str]) -> Tuple[Set[str], Set[str]]:
    build_id_query = """
    select tablename
    from pg_catalog.pg_tables
    where schemaname='public' and tablename like 'build_id%';
    """
    cursor.execute(build_id_query)
    build_id_retain, build_id_trim = partition_set_by_filter(
        {row[0] for row in cursor.fetchall()}, retain_suffix_set
    )
    display_summary("retain", build_id_retain)
    display_summary("trim", build_id_trim)
    return build_id_retain, build_id_trim


def trim_tables(conn, trim_set: Set[str], batch_size=100):
    cursor = conn.cursor()
    trim_list = list(trim_set)
    num_batches = (len(trim_list) // batch_size) + 1
    for i in range(num_batches):
        trim_subset = trim_list[i * batch_size : (i + 1) * batch_size]
        if not trim_subset:
            continue
        print(f"dropping {i+1} out of {num_batches} batches in groups of {batch_size}")
        tables = ", ".join(trim_subset)
        query = f"drop table {tables};"
        cursor.execute(query)
        conn.commit()


@click.command()
@click.option(
    "--base-date", type=str, default=datetime.strftime(datetime.today(), DS_NODASH)
)
@click.option("--retention-period", type=int, default=365 * 2)
@click.option("--dry-run/--no-dry-run", default=True)
@click.option("--postgres-db", type=str, envvar="POSTGRES_DB", default="telemetry")
@click.option("--postgres-user", type=str, envvar="POSTGRES_USER", default="root")
@click.option("--postgres-pass", type=str, envvar="POSTGRES_PASS", required=True)
@click.option("--postgres-host", type=str, envvar="POSTGRES_HOST", required=True)
def main(
    base_date,
    retention_period,
    dry_run,
    postgres_db,
    postgres_user,
    postgres_pass,
    postgres_host,
):
    conn = create_connection(postgres_db, postgres_user, postgres_pass, postgres_host)
    cursor = conn.cursor()

    retain_suffix_set = retention_date_range(base_date, retention_period)
    submission_retain, submission_trim = query_submission_date(
        cursor, retain_suffix_set
    )
    build_id_retain, build_id_trim = query_build_id(cursor, retain_suffix_set)

    if not dry_run:
        print("Dropping tables...")
        trim_tables(conn, submission_trim | build_id_trim)
    else:
        print("Dry run enabled, not dropping tables...")
    conn.close()


if __name__ == "__main__":
    main()
