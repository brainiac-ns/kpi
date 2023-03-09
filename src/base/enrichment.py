import pandas as pd


class Enrichment:
    def load_supplier():
        return pd.read_parquet("landing/supplier")

    def load_invoice():
        return pd.read_parquet("landing/invoice")

    def load_ifa_master():
        return pd.read_parquet("landing/ifa_master")
