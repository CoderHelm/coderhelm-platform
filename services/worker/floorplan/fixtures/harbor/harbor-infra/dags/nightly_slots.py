from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_slots():
    hook = PostgresHook(postgres_conn_id="harbor-db")
    hook.run("INSERT INTO slot (id, name) SELECT id, name FROM staging_slots")
