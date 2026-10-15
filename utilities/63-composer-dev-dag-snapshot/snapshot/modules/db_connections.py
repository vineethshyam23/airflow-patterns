from airflow.models import Variable
import psycopg2
import logging
import os

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def initialize_db_connection(env: str = env):
    """Initialize the database connection using psycopg2."""
    try:
        if env == "PROD":
            postgres_details = Variable.get(
                "alloydb_dev_details", deserialize_json=True #todo change to prod
            )
        else:
            postgres_details = Variable.get(
                "alloydb_dev_details", deserialize_json=True
            )
        conn = psycopg2.connect(
            host=postgres_details["host"],
            database=postgres_details["database"],
            user=postgres_details["user"],
            password=postgres_details["password"],
            port=postgres_details["port"],
        )
        conn.autocommit = True
    except Exception as e:
        logging.error(f"Failed to establish database connection: {str(e)}")
        raise
    logging.info("Successfully established database connection")
    return conn