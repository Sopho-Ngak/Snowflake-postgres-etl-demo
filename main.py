from etl import ETL, TABLES





if __name__ == '__main__':
    etl = ETL()
    for table in TABLES:
        df = etl.extract(table=table)
        transformed_df = etl.transform(df=df, table=table)

        etl.load_to_postgres(transformed_df, table=table)