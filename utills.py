from sqlalchemy import create_engine
import snowflake.connector

import config as settings


class Connections:
    def connect_to_snowflake(self):
        try:
            print("====> Connecting to snowflake =====>")
            conn = snowflake.connector.connect(
                user=settings.snow_flake_user,
                password=settings.snow_flake_password,
                account=settings.snow_flake_account,
                database=settings.snow_flake_db

            )
            print("===========> Connected to Snowflake")
            return conn
        except Exception as e:
            raise e
        
    
    def connect_to_postgres(self):

        try:
            print("=====> Connecting to postgres ====>")
            engine = create_engine(f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}")
            conn = engine.connect()

            print("==> Connected to postgres")
            return conn
        
        except Exception as e:
            raise e


if __name__ == '__main__':
    conn = Connections()

    conn.connect_to_snowflake()
    conn.connect_to_postgres()