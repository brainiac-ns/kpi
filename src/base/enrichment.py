import pandas as pd

from models.enrichment_data.ifa_invoices import IfaInvoices
from models.input_data.ifa_master import IfaMaster
from models.input_data.supplier import Supplier


class Enrichment:
    def __init__(self):
        self.df_invoice: pd.DataFrame = None
        self.df_supplier: pd.DataFrame = None
        self.df_ifa_master: pd.DataFrame = None

    def load_supplier(self):
        return pd.read_parquet("landing/supplier")

    def load_invoice(self):
        return pd.read_parquet("landing/invoice")

    def load_ifa_master(self):
        return pd.read_parquet("landing/ifa_master")

    def merge_invoice_supplier(self, column_name: str) -> pd.DataFrame:
        self.df_invoice[IfaInvoices.budat.name] = self.df_invoice[IfaInvoices.budat.name].astype("datetime64[ns]")
        self.df_supplier[Supplier.valid_to.name] = self.df_supplier[Supplier.valid_to.name].astype("datetime64[ns]")
        self.df_supplier[Supplier.valid_from.name] = self.df_supplier[Supplier.valid_from.name].astype("datetime64[ns]")
        self.df_ifa_master[IfaMaster.deletion_date.name] = self.df_ifa_master[IfaMaster.deletion_date.name].astype(
            "datetime64[ns]"
        )

        df_join = pd.merge(
            self.df_invoice,
            self.df_supplier,
            how="left",
            on=[
                IfaInvoices.logsys.name,
                self.df_invoice[column_name] == self.df_supplier[Supplier.lifnr.name],
                # self.df_invoice[IfaInvoices.budat.name].dt.date > self.df_supplier[Supplier.valid_from.name].dt.date,
                # self.df_invoice[IfaInvoices.budat.name].dt.date < self.df_supplier[Supplier.valid_to.name].dt.date,
            ],
        )
        print(df_join)
        df_join = df_join[df_join["key_1"] == True]
        # df_join = df_join[df_join["key_2"] == True]
        # df_join = df_join[df_join["key_3"] == True]

        return df_join
