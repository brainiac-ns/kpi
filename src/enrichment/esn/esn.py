import datetime
import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text as sql_text

from enrichment.base.cdc import CDC
from enrichment.base.enrichment import Enrichment
from models.enrichment_data.buo_target import BuoTarget
from models.enrichment_data.esn_target import EsnTarget
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.enrichment_data.ifa_target import IfaTarget
from models.input_data.organization import Organization


class Esn(Enrichment):
    def __init__(self, logsys_list="*", timeframe_start="*", timeframe_end="*"):
        super(Esn, self).__init__()
        self.df_invoice = self.filter_dataframe(self.load_invoice(), logsys_list, timeframe_start, timeframe_end)
        self.df_ifa = self.load_ifa_filtered(logsys_list, timeframe_start, timeframe_end)
        self.df_buo = self.load_buo_filtered(logsys_list, timeframe_start, timeframe_end)
        self.df_organization = self.load_organization()

    def __call__(self):
        self.df_invoice[IfaInvoices.budat.name] = pd.to_datetime(
            self.df_invoice[IfaInvoices.budat.name], format="%Y/%m/%d"
        )
        df_invoice_buo = pd.merge(
            self.df_invoice, self.df_buo, how="left", on=[IfaTarget.logsys.name, IfaTarget.budat.name]
        )

        df_invoice_buo[EsnTarget.esn.name] = df_invoice_buo[BuoTarget.buo.name]

        df_join = pd.merge(df_invoice_buo, self.df_ifa, how="left", on=[IfaTarget.logsys.name, IfaTarget.budat.name])

        df_join.loc[df_join[IfaTarget.ifacmd_filkd.name] != "#", EsnTarget.esn.name] = 3
        df_join.loc[df_join[IfaTarget.ifacmd_krenr.name] != "#", EsnTarget.esn.name] = 2
        df_join.loc[df_join[IfaTarget.ifacmd_lifnr.name] != "#", EsnTarget.esn.name] = 1

        self.df_organization.drop(columns=["active_flag", "published_from", "published_to"], inplace=True)

        df_join_org = pd.merge(
            df_join,
            self.df_organization,
            how="left",
            left_on=BuoTarget.buo.name,
            right_on=Organization.organization_id.name,
        )

        df_join_org = df_join_org[~df_join_org[Organization.parent_hierarchy_ids.name].isna()]
        df_join_org[EsnTarget.esn.name] = df_join_org.apply(self.check_value, axis=1)

        esn_fields = [string for string in dir(EsnTarget) if "__" not in string]
        selected_cols = []
        for field in esn_fields:
            print(field)
            if field in df_join_org.columns.tolist():
                selected_cols.append(field)
            else:
                selected_cols.append(f"{field}_x")

        df_join_org[Organization.active_flag.name] = df_join_org["active_flag_x"]
        df_join_org = df_join_org.loc[:, selected_cols]

        df_join_org = df_join_org.iloc[[0]]

        table_name = "enr_esn"
        pk_columns = [EsnTarget.logsys.name, EsnTarget.budat.name]
        enr_columns = [EsnTarget.esn.name]
        update_columns = {
            EsnTarget.active_flag.name: False,
            EsnTarget.published_to.name: "'" + datetime.datetime.now().strftime("%Y-%m-%d") + "'",
        }
        insert_timestamp_column = EsnTarget.published_from.name
        active_flag_column = EsnTarget.active_flag.name

        cdc = CDC(
            df_join_org,
            table_name,
            pk_columns,
            enr_columns,
            update_columns,
            insert_timestamp_column,
            active_flag_column,
        )
        cdc()

        return df_join_org

    def load_ifa_filtered(self, logsys_list, timeframe_start, timeframe_end) -> pd.DataFrame:
        host_ip = os.environ.get("HOST_IP", "localhost")
        engine = create_engine(f"postgresql://user:admin@{host_ip}:54320/postgres")

        query = "SELECT * FROM ifa_target"

        df_ifa = pd.read_sql_query(con=engine.connect(), sql=sql_text(query))

        df_ifa = self.filter_dataframe(df_ifa, logsys_list, timeframe_start, timeframe_end)

        return df_ifa

    def load_buo_filtered(self, logsys_list, timeframe_start, timeframe_end) -> pd.DataFrame:
        host_ip = os.environ.get("HOST_IP", "localhost")
        engine = create_engine(f"postgresql://user:admin@{host_ip}:54320/postgres")

        query = "SELECT * FROM enr_buo"

        df_buo = pd.read_sql_query(con=engine.connect(), sql=sql_text(query))
        df_buo = self.filter_dataframe(df_buo, logsys_list, timeframe_start, timeframe_end)
        df_buo[BuoTarget.budat.name] = pd.to_datetime(df_buo[BuoTarget.budat.name], format="%Y/%m/%d")

        return df_buo

    def filter_dataframe(self, df, logsys_list, timeframe_start, timeframe_end) -> pd.DataFrame:
        df = df[df[IfaTarget.active_flag.name] == "active"]

        if logsys_list != "*":
            df = df[df[IfaTarget.logsys.name] in logsys_list]

        if timeframe_start != "*":
            df = df[df[IfaTarget.budat.name] >= timeframe_start]

        if timeframe_end != "*":
            df = df[df[IfaTarget.budat.name] < timeframe_end]

        return df

    def check_value(self, row):
        if (
            row[IfaInvoices.valid_org.name] is None
            or row[IfaInvoices.valid_org.name] in row[Organization.parent_hierarchy_ids.name]
        ):
            return 4
        else:
            return row[EsnTarget.esn.name]
