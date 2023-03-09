import pandas as pd


class Enrichment:
    def load_supplier(self):
        return pd.read_parquet("landing/supplier")

    def load_invoice(self):
        return pd.read_parquet("landing/invoice")

    def load_ifa_master(self):
        return pd.read_parquet("landing/ifa_master")
