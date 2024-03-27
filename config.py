import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))


postgres_user = os.environ['DB_USER']
postgres_password = os.environ['DB_PASSWORD']
postgres_host = os.environ['DB_HOST']
postgres_db = os.environ['DBT_NAME']
postgres_port = os.environ['DB_PORT']


snow_flake_user = os.environ['SNOWFLAKE_USER']
snow_flake_password = os.environ['SNOWFLAKE_PASSWORD']
snow_flake_account = os.environ['SNOWFLAKE_ACCOUNT']
snow_flake_db = os.environ['SNOWFLAKE_DATABASE']





