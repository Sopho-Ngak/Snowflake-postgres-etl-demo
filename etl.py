import pandas as pd
from utills import Connections
import datetime


TABLES = [
    # 'CALL_CENTER',
    'CUSTOMER'
]

class ETL(Connections):
    def __init__(self):
        self.target_schema = 'demo'
        self.source_schema = 'TPCDS_SF100TCL'

    def extract(self, table: str) -> pd.DataFrame:
        conn = self.connect_to_snowflake()
        cursor = conn.cursor()

        print("==========> Getting data from snowflake =============>")
        cursor.execute(f'SELECT * FROM {self.source_schema}.{table};')

        df = cursor.fetch_pandas_all()
        conn.close()
        print("______________Done____________")
        # df = pd.read_sql(f'SELECT * FROM {self.source_schema}.{table}', con=conn)
        return df
    
    def transform(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        print("============> Cleaning data =>")
        def convert_julian_date_to_gregorian_date(julian_date):
            '''
            Julian dates to Gregorian

            a = julian_date + 32044: This step calculates a modified Julian date, adding an offset of 32044. This offset is used to align the starting point of the Julian and Gregorian calendars.

            b = (4 * a + 3) // 146097: Here, we calculate a value b which represents the number of 400-year cycles that have passed since the start of the Gregorian calendar (since the year 0). This calculation adjusts for the leap years.

            c = a - ((146097 * b) // 4): We calculate a value c which represents the day count within the current 400-year cycle.

            d = (4 * c + 3) // 1461: This step calculates a value d representing the number of 100-year cycles that have passed within the current 400-year cycle.

            e = c - ((1461 * d) // 4): We calculate a value e which represents the day count within the current 100-year cycle.

            m = (5 * e + 2) // 153: This step calculates a value m representing the month. It's based on the day count within the 100-year cycle.

            day = e - ((153 * m + 2) // 5) + 1: We calculate the day of the month by subtracting a calculated value from e. This step corrects for the day count within the current month.

            month = m + 3 - 12 * (m // 10): Here, we calculate the month. This step adjusts for the fact that the year starts in March under the Julian calendar.

            year = 100 * b + d - 4800 + (m // 10): Finally, we calculate the year. This step adjusts for the fact that the year 0 is represented by the year 4800 BCE in the Gregorian calendar.
            '''
            try:
                julian_date = int(julian_date)
                date = datetime.datetime.fromordinal(julian_date + 1721425)
                return date
            except ValueError:
                a = julian_date + 32044
                b = (4 * a + 3) // 146097
                c = a - (146097 * b) // 4
                d = (4 * c + 3) // 1461
                e = c - (1461 * d) // 4
                m = (5 * e + 2) // 153
                day = e - (153 * m + 2) // 5 + 1
                month = m + 3 - 12 * (m // 10)
                year = 100 * b + d - 4800 + m // 10
                return datetime.date(year, month, day)
            except TypeError :
                return pd.NaT
            
            except Exception as e:
                raise e

        if table == 'CALL_CENTER':
            df.columns = map(str.lower, df.columns)

            df.rename(columns={
                'cc_call_center_id': 'id',
                'cc_call_center_sk': 'unique_id',
                'cc_city': 'city',
                'cc_rec_start_date': 'start_date',
                'cc_rec_end_date': 'end_date',
                'cc_name': 'name',
                'cc_class': 'class',
                'cc_employees': 'employees',
                'cc_sq_ft': 'sq_ft',
                'cc_hours': 'hours',
                'cc_manager': 'line_manager',
                'cc_mkt_id': 'mkt_id',
                'cc_mkt_class': 'mkt_class',
                'cc_mkt_desc': 'mkt_desc',
                'cc_market_manager': 'market_manager',
                'cc_division': 'division_id',
                'cc_division_name': 'division_name',
                'cc_company': 'company_id',
                'cc_company_name': 'company_name',
                'cc_street_number': 'street_number',
                'cc_street_name': 'street_name',
                'cc_street_type': 'street_type',
                'cc_suite_number': 'suite_number',
                'cc_city': 'city',
                'cc_county': 'county',
                'cc_state': 'state',
                'cc_zip': 'zip',
                'cc_country': 'country',
                'cc_gmt_offset': 'offset',
                'cc_tax_percentage': 'tax_percentage',
                'cc_open_date_sk': 'open_date',
            }, inplace=True)

            df['start_date'] = pd.to_datetime(df['start_date'], format='%Y-%m-%d %H:%M:%S')
            df['end_date'] = pd.to_datetime(df['end_date'], format='%Y-%m-%d %H:%M:%S')
            df['open_date'].apply(convert_julian_date_to_gregorian_date)
            df['open_date'] = pd.to_datetime(df['open_date'], format='%Y-%m-%d %H:%M:%S')

        if table == 'CUSTOMER':
            df.columns = map(str.lower, df.columns)

            df.rename(columns={
                'c_customer_id': 'id',
                'c_customer_sk': 'unique_id',
                'c_current_cdemo_sk': 'current_cdemo_sk',
                'c_current_hdemo_sk': 'current_hdemo_sk',
                'c_current_addr_sk': 'current_addr_sk',
                'c_first_shipto_date_sk': 'first_shipto_date_sk',
                'c_first_sales_date_sk': 'first_sales_date_sk',
                'c_salutation': 'salutation',
                'c_first_name': 'first_name',
                'c_last_name': 'last_name',
                'c_preferred_cust_flag': 'preferred_cust_flag',
                'c_birth_day': 'birth_day',
                'c_birth_month': 'birth_month',
                'c_birth_year': 'birth_year',
                'c_birth_country': 'birth_country',
                'c_login': 'login',
                'c_email_address': 'email_address',
                'c_last_review_date': 'last_review_date',
            }, inplace=True)

            df['c_birth_day'].astype(int)
            df['c_birth_month'].astype(int)
            df['c_birth_year'].astype(int)

            df.drop(columns=['login'], inplace=True)
            # df['birth_date'] = pd.to_datetime(df[['birth_year', 'birth_month' 'birth_day']].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d')
            # df['birth_date'] = pd.to_datetime(df[['birth_year', 'birth_month', 'birth_day']].astype(str))
            df['birth_date'] = pd.to_datetime(df['birth_year'].astype(str) + '-' + df['birth_month'].astype(str) + '-' + df['birth_day'].astype(str), format='%Y-%m-%d')

            df.drop(columns=['birth_year', 'birth_month' 'birth_day'], inplace=True)

            # Covert to real date column
            df['last_review_date'] = df['last_review_date'].apply(convert_julian_date_to_gregorian_date)
            df['last_review_date'] = pd.to_datetime(df['last_review_date'], format='%Y-%m-%d %H:%M:%S')

            df['first_shipto_date_sk'] = df['first_shipto_date_sk'].apply(convert_julian_date_to_gregorian_date)
            df['first_shipto_date_sk'] = pd.to_datetime(df['first_shipto_date_sk'], format='%Y-%m-%d %H:%M:%S')

            df['first_sales_date_sk'] = df['first_sales_date_sk'].apply(convert_julian_date_to_gregorian_date)
            df['first_sales_date_sk'] = pd.to_datetime(df['first_sales_date_sk'], format='%Y-%m-%d %H:%M:%S')
            





        print("__________Done_______________")
        return df
    

    def load_to_postgres(self, df: pd.DataFrame, table: str) -> None:
        conn = self.connect_to_postgres()
        df['loadded_date'] = pd.Timestamp.now()

        # df.to_csv(f'{table}.csv', index=False)
        
        print("Loadding into Postgres")
        df.to_sql(name=table.lower(), con=conn, schema=self.target_schema, if_exists='append', index=False)
        print("_____________Done______________")




