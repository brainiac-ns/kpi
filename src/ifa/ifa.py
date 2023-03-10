import pandas as pd
from sqlalchemy import create_engine

from base.enrichment import Enrichment
from constants import Constants
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
        df_lifnr = self.calculate_kpi(IfaInvoices.lifnr.name, IfaTarget.ifacmd_lifnr.name)
        df_krenr = self.calculate_kpi(IfaInvoices.krenr.name, IfaTarget.ifacmd_krenr.name)
        df_filkd = self.calculate_kpi(IfaInvoices.filkd.name, IfaTarget.ifacmd_filkd.name)

        df_merge = pd.merge(df_lifnr, df_krenr)
        df_merge = pd.merge(df_merge, df_filkd)

        engine = create_engine("postgresql://user:admin@localhost:54320/postgres")

        df_merge.to_sql(name="ifa_target", con=engine, if_exists="replace", index=False)

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


ifa = Ifa()
ifa()
