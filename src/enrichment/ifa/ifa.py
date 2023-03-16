import datetime

import pandas as pd

from constants import Constants
from enrichment.base.cdc import CDC
from enrichment.base.enrichment import Enrichment
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.enrichment_data.ifa_target import IfaTarget
from models.input_data.ifa_master import IfaMaster
from models.input_data.supplier import Supplier


class Ifa(Enrichment):
    def __init__(self):
        super(Ifa, self).__init__()
        self.df_invoice = self.load_invoice()
        self.df_supplier = self.load_supplier()
        self.df_ifa_master = self.load_ifa_master()

    def __call__(self):
        print("ifa1")
        df_lifnr = self.calculate_kpi(IfaInvoices.lifnr.name, IfaTarget.ifacmd_lifnr.name)
        df_krenr = self.calculate_kpi(IfaInvoices.krenr.name, IfaTarget.ifacmd_krenr.name)
        df_filkd = self.calculate_kpi(IfaInvoices.filkd.name, IfaTarget.ifacmd_filkd.name)
        print("ifa2")

        df_merge = pd.merge(df_lifnr, df_krenr)
        df_merge = pd.merge(df_merge, df_filkd)
        print("ifa3")

        selected_cols = [string for string in dir(IfaTarget) if "__" not in string]

        df_merge = df_merge.loc[:, selected_cols]

        table_name = Constants.IFA_TARGET.value
        print("ifa4")

        pk_columns = [IfaInvoices.logsys.name, IfaInvoices.budat.name]
        enr_columns = [IfaTarget.ifacmd_filkd.name, IfaTarget.ifacmd_krenr.name, IfaTarget.ifacmd_lifnr.name]
        update_column = {
            IfaTarget.active_flag.name: False,
            IfaTarget.published_to.name: "'" + datetime.datetime.now().strftime("%Y-%m-%d") + "'",
        }
        insert_timestamp_column = IfaInvoices.published_from.name
        active_flag_column = IfaInvoices.active_flag.name
        print("ifa5")

        cdc = CDC(
            df_merge,
            table_name,
            pk_columns,
            enr_columns,
            update_column,
            insert_timestamp_column,
            active_flag_column,
        )
        print("ifa6")

        cdc()
        print("ifa7")

        return df_merge

    def calculate_kpi(self, column_name: str, target_column: str) -> pd.DataFrame:
        df_join = self.merge_invoice_supplier(column_name)

        max_idx = df_join.groupby([IfaInvoices.logsys.name, column_name])[f"{Supplier.published_from.name}_y"].idxmax()
        df_join = df_join.loc[max_idx]

        df_join[Constants.IFASAP.value] = df_join[Supplier.ifanr.name]

        self.df_ifa_master[IfaMaster.deletion_date.name] = self.df_ifa_master[IfaMaster.deletion_date.name].astype(
            "datetime64[ns]"
        )

        df_join_ifa = pd.merge(
            df_join,
            self.df_ifa_master,
            how="left",
            left_on=Constants.IFASAP.value,
            right_on=IfaMaster.ifanr.name,
        )

        df_join_ifa[target_column] = "#"

        df_join_ifa.loc[
            df_join_ifa[IfaMaster.loekz.name].notna()
            & (
                ~(
                    (df_join_ifa[IfaMaster.loekz.name] == True)
                    & (df_join_ifa[IfaMaster.deletion_date.name] < df_join_ifa[IfaInvoices.budat.name])
                )
                | (df_join_ifa[IfaMaster.loekz.name] == False)
            ),
            target_column,
        ] = df_join_ifa[Constants.IFASAP.value]

        return df_join_ifa
