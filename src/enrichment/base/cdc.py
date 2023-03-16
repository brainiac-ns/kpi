import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text as sql_text

from constants import Constants
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.enrichment_data.ifa_target import IfaTarget


class CDC:
    def __init__(
        self,
        new_data_df: pd.DataFrame,
        table_name: str,
        pk_columns: list,
        enr_columns: list,
        update_columns: dict,
        insert_timestamp_column: str,
        active_flag_column: str,
    ):
        self.new_data_df = new_data_df
        self.table_name = table_name
        self.pk_columns = pk_columns
        self.enr_columns = enr_columns
        self.update_columns = update_columns
        self.insert_timestamp_column = insert_timestamp_column
        self.active_flag_column = active_flag_column

    def __call__(self):
        engine = create_engine("postgresql://user:admin@localhost:54320/postgres")

        query = f"SELECT * FROM {self.table_name}"
        old_data_df = pd.read_sql_query(con=engine.connect(), sql=sql_text(query))
        old_data_df[Constants.IS_NEW_RECORD.value] = False
        self.new_data_df[Constants.IS_NEW_RECORD.value] = True

        merged_df = pd.concat([old_data_df, self.new_data_df])
        print(merged_df)
        merged_df[Constants.COUNT_ROWS.value] = merged_df.groupby(self.pk_columns + self.enr_columns)[
            IfaInvoices.active_flag.name
        ].transform("count")
        print(merged_df)
        filtered_df = merged_df[
            (merged_df[Constants.COUNT_ROWS.value] == 1) & (merged_df[Constants.IS_NEW_RECORD.value] == True)
        ]
        old_data_df = old_data_df.drop(Constants.IS_NEW_RECORD.value, axis=1)
        filtered_df = filtered_df.drop([Constants.COUNT_ROWS.value, Constants.IS_NEW_RECORD.value], axis=1)
        print(filtered_df)
        # ----
        update_statement = ""
        join_filter = ""
        for key_column in self.pk_columns:
            join_filter = join_filter + f""" TARGET_TABLE.{key_column} = LAST_UPDATED.{key_column} AND"""

        join_filter = join_filter[:-3]
        for key in self.update_columns:
            update_statement = update_statement + f""" {key} = {self.update_columns[key]},"""

        update_statement = update_statement[:-1]
        search_columns = self.pk_columns + self.enr_columns
        select_keys = ",".join(search_columns)

        update_condition = (
            f"""TARGET_TABLE.{self.insert_timestamp_column} < LAST_UPDATED.{self.insert_timestamp_column}"""
        )
        where_clause = f"WHERE {self.active_flag_column} = 'active'"
        # ???
        # logsys = "logsys1"

        # where_clause = f"{where_clause} AND LOGSYS = '{logsys}'"

        last_updated_table = (
            f"""SELECT {select_keys}, {self.insert_timestamp_column} FROM {self.table_name} {where_clause}"""
        )

        update_existed_rows_stm = f"""MERGE INTO {self.table_name} TARGET_TABLE USING( {last_updated_table} ) \
        LAST_UPDATED ON ({join_filter} AND TARGET_TABLE.{self.active_flag_column} = 'active') WHEN MATCHED AND \
        {update_condition} THEN UPDATE SET {update_statement}"""

        # ----

        existing_table = old_data_df[old_data_df[IfaTarget.active_flag.name] == True]
        # ???
        # existing_table = existing_table[existing_table[IfaTarget.logsys.name] == logsys]
        common_cols = list(set(filtered_df.columns).intersection(set(existing_table.columns)))
        filtered_df = filtered_df.loc[:, common_cols].drop_duplicates()
        filtered_df[IfaTarget.published_to.name] = None
        filtered_df[IfaTarget.published_from.name] = datetime.datetime.now().strftime("%Y-%m-%d")
        filtered_df[IfaTarget.active_flag.name] = "active"
        filtered_df.to_sql(name=self.table_name, con=engine, if_exists="append", index=False)
        # update_column = {
        #     IfaTarget.active_flag.name: False,
        #     IfaTarget.published_to.name: "'" + datetime.now().strftime("%Y-%m-%d") + "'",
        # }

        with engine.begin() as conn:
            conn.execute(sql_text(update_existed_rows_stm))
            conn.commit()
