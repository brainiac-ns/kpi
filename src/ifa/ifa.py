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
        self.df_invoice[IfaInvoices.budat.name] = self.df_invoice[IfaInvoices.budat.name].astype("datetime64[ns]")
        self.df_supplier[Supplier.valid_to.name] = self.df_supplier[Supplier.valid_to.name].astype("datetime64[ns]")
        self.df_supplier[Supplier.valid_from.name] = self.df_supplier[Supplier.valid_from.name].astype("datetime64[ns]")
        self.df_ifa_master[IfaMaster.deletion_date.name] = self.df_ifa_master[IfaMaster.deletion_date.name].astype(
            "datetime64[ns]"
        )

        # self.df_supplier["interval"] = self.df_supplier.apply(
        #     lambda row: pd.Interval(row[Supplier.valid_from.name], row[Supplier.valid_to.name], closed="both"), axis=1
        # )

        df_join = pd.merge(
            self.df_invoice,
            self.df_supplier,
            how="left",
            on=[
                IfaInvoices.logsys.name,
                self.df_invoice[column_name] == self.df_supplier[Supplier.lifnr.name],
                self.df_invoice[IfaInvoices.budat.name].dt.date > self.df_supplier[Supplier.valid_from.name].dt.date,
                self.df_invoice[IfaInvoices.budat.name].dt.date < self.df_supplier[Supplier.valid_to.name].dt.date,
            ]
            # left_on=[
            #     IfaInvoices.logsys.name,
            #     column_name,
            #     df_invoice[IfaInvoices.budat.name] < df_supplier[Supplier.valid_from.name],
            #     df_invoice[IfaInvoices.budat.name].apply(lambda x: x in df_supplier["interval"])
            #     df_invoice[IfaInvoices.budat.name].between(
            #         df_supplier[Supplier.valid_from.name], df_supplier[Supplier.valid_to.name]
            #     ),
            # ],
            # right_on=[Supplier.logsys.name, Supplier.lifnr.name],
        )
        df_join = df_join[df_join["key_1"] == True]
        df_join = df_join[df_join["key_2"] == True]

        df_join[Constants.IFASAP.value] = df_join[Supplier.ifanr.name]

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
