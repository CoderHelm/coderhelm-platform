from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_slips():
    hook = PostgresHook(postgres_conn_id="harbor-db")
    hook.run("INSERT INTO slip (id, name) SELECT id, name FROM staging_slips")
