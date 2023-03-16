import os

import pandas as pd
import yaml

from models.base.table import Table
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.input_data.ifa_master import IfaMaster
from models.input_data.organization import Organization
from models.input_data.supplier import Supplier
from pipeline_job_config import PipelineJobConfig
from utils import deserialize_json


class PreprocessingJob:
    def __init__(self, path):
        self.path = path
        with open(path, "r") as stream:
            d = yaml.safe_load(stream)
        self.config: PipelineJobConfig = deserialize_json(PipelineJobConfig, d)

    def __call__(self):
        print("job1")
        for i in os.listdir(self.config.input_data_path):
            df_csv = pd.read_csv(f"{self.config.input_data_path}/{i}/{i}.csv")

            if i == "organization":
                df_csv[Organization.parent_hierarchy_ids.name] = df_csv[
                    Organization.parent_hierarchy_ids.name
                ].str.split(";")

            df_json = pd.read_json(f"{self.config.input_data_path}/{i}/{i}.json")
            df_union = pd.concat([df_csv, df_json])
            df_union[Table.published_from.name] = pd.to_datetime(df_union[Table.published_from.name], format="%Y/%m/%d")
            df_union[Table.published_to.name] = pd.to_datetime(df_union[Table.published_to.name], format="%Y/%m/%d")

            if i == "invoice":
                df_union[IfaInvoices.budat.name] = pd.to_datetime(df_union[IfaInvoices.budat.name], format="%Y/%m/%d")

            if i == "supplier":
                df_union[Supplier.valid_from.name] = pd.to_datetime(
                    df_union[Supplier.valid_from.name], format="%Y/%m/%d"
                )
                df_union[Supplier.valid_to.name] = pd.to_datetime(df_union[Supplier.valid_to.name], format="%Y/%m/%d")

            if i == "organization":
                df_union[Organization.valid_from.name] = pd.to_datetime(
                    df_union[Organization.valid_from.name], format="%Y/%m/%d"
                )
                df_union[Organization.valid_to.name] = pd.to_datetime(
                    df_union[Organization.valid_to.name], format="%Y/%m/%d"
                )

            if i == "ifa_master":
                df_union[IfaMaster.deletion_date.name] = pd.to_datetime(
                    df_union[IfaMaster.deletion_date.name], format="%Y/%m/%d"
                )

            partition_cols = ""
            if i == "invoice":
                partition_cols = IfaInvoices.budat.name
            elif i == "supplier":
                partition_cols = Supplier.logsys.name
            elif i == "ifa_master":
                partition_cols = IfaMaster.ifanr.name
            else:
                partition_cols = Organization.organization_type.name

            df_union.to_parquet(f"{self.config.output_data_path}/{i}", partition_cols=partition_cols)
