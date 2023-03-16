import pandas as pd

from models import IfaInvoices
from models.input_data.supplier import Supplier


class Enrichment:
    def __init__(self):
        self.df_invoice: pd.DataFrame = None
        self.df_supplier: pd.DataFrame = None
        self.df_ifa_master: pd.DataFrame = None
        self.df_organization: pd.DataFrame = None

    def load_supplier(self):
        return pd.read_parquet("data/landing/supplier")

    def load_invoice(self):
        return pd.read_parquet("data/landing/invoice")

    def load_ifa_master(self):
        return pd.read_parquet("data/landing/ifa_master")

    def load_organization(self):
        return pd.read_parquet("data/landing/organization")

    def merge_invoice_supplier(self, column_name: str) -> pd.DataFrame:
        self.df_invoice[IfaInvoices.budat.name] = self.df_invoice[IfaInvoices.budat.name].astype("datetime64[ns]")
        self.df_supplier[Supplier.valid_to.name] = self.df_supplier[Supplier.valid_to.name].astype("datetime64[ns]")
        self.df_supplier[Supplier.valid_from.name] = self.df_supplier[Supplier.valid_from.name].astype("datetime64[ns]")

        if column_name != Supplier.lifnr.name:
            self.df_supplier = self.df_supplier.rename(columns={Supplier.lifnr.name: column_name})

        df_join = pd.merge(
            self.df_invoice,
            self.df_supplier,
            how="left",
            on=[
                IfaInvoices.logsys.name,
                column_name,
            ],
        )

        if column_name != Supplier.lifnr.name:
            self.df_supplier = self.df_supplier.rename(columns={column_name: Supplier.lifnr.name})

        df_join = df_join[df_join[IfaInvoices.budat.name] > df_join[Supplier.valid_from.name]]
        df_join = df_join[df_join[IfaInvoices.budat.name] < df_join[Supplier.valid_to.name]]

        return df_join
