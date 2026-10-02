"""
Database credentials management module
"""

import os
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_db_connection_params(env="PROD"):
    """
    Get database connection parameters based on environment

    Args:
        env (str): Environment (DEV, PROD)

    Returns:
        dict: Database connection parameters
    """
    if env == "DEV":
        return {
            "host": Variable.get(
                "ALLOYDB_DEV_HOST", default_var=os.getenv("ALLOYDB_DEV_HOST")
            ),
            "port": Variable.get(
                "ALLOYDB_DEV_PORT", default_var=os.getenv("ALLOYDB_DEV_PORT", "5432")
            ),
            "database": Variable.get(
                "ALLOYDB_DEV_DATABASE", default_var=os.getenv("ALLOYDB_DEV_DATABASE")
            ),
            "username": Variable.get(
                "ALLOYDB_DEV_USERNAME", default_var=os.getenv("ALLOYDB_DEV_USERNAME")
            ),
            "password": Variable.get(
                "ALLOYDB_DEV_PASSWORD", default_var=os.getenv("ALLOYDB_DEV_PASSWORD")
            ),
        }
    else:
        return {
            "host": Variable.get(
                "ALLOYDB_PROD_HOST", default_var=os.getenv("ALLOYDB_PROD_HOST")
            ),
            "port": Variable.get(
                "ALLOYDB_PROD_PORT", default_var=os.getenv("ALLOYDB_PROD_PORT", "5432")
            ),
            "database": Variable.get(
                "ALLOYDB_PROD_DATABASE", default_var=os.getenv("ALLOYDB_PROD_DATABASE")
            ),
            "username": Variable.get(
                "ALLOYDB_PROD_USERNAME", default_var=os.getenv("ALLOYDB_PROD_USERNAME")
            ),
            "password": Variable.get(
                "ALLOYDB_PROD_PASSWORD", default_var=os.getenv("ALLOYDB_PROD_PASSWORD")
            ),
        }


def create_connection_string(env="PROD"):
    """
    Create PostgreSQL connection string

    Args:
        env (str): Environment (DEV, PROD)

    Returns:
        str: PostgreSQL connection string
    """
    params = get_db_connection_params(env)
    return f"postgresql://{params['username']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"


def get_postgres_hook(env="PROD"):
    """
    Get PostgresHook with credentials

    Args:
        env (str): Environment (DEV, PROD)

    Returns:
        PostgresHook: Configured PostgresHook instance
    """
    params = get_db_connection_params(env)
    return PostgresHook(
        postgres_conn_id=None,  # We'll pass connection details directly
        host=params["host"],
        port=params["port"],
        database=params["database"],
        username=params["username"],
        password=params["password"],
    )
