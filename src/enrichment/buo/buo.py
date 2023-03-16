import datetime

import pandas as pd

from constants import Constants
from enrichment.base.cdc import CDC
from enrichment.base.enrichment import Enrichment
from models.enrichment_data.buo_target import BuoTarget
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.input_data.organization import Organization
from models.input_data.supplier import Supplier


class Buo(Enrichment):
    def __init__(self):
        super(Buo, self).__init__()
        self.df_invoice = self.load_invoice()
        self.df_supplier = self.load_supplier()
        self.df_organization = self.load_organization()

    def __call__(self):
        print("buo")
        df_join = self.merge_invoice_supplier(IfaInvoices.lifnr.name)

        max_idx = self.df_organization.groupby(Organization.supplier_org.name)[
            Organization.published_from.name
        ].idxmax()
        self.df_organization = self.df_organization.loc[max_idx]
        print("buo")

        df_join_org = pd.merge(
            df_join,
            self.df_organization,
            how="left",
            left_on=Supplier.buo_org.name,
            right_on=Organization.supplier_org.name,
        )
        print("buo")

        df_join_org[Constants.BUO.value] = Constants.DUMMY_BUO.value
        df_join_org.loc[df_join_org[Organization.organization_id.name].notna(), Constants.BUO.value] = df_join_org[
            Organization.organization_id.name
        ]
        print("buo")

        df_join_org[Organization.parent_hierarchy_ids.name] = df_join_org[Organization.parent_hierarchy_ids.name].apply(
            lambda x: str(x)
        )
        print("buo")

        selected_cols = [string for string in dir(BuoTarget) if "__" not in string]

        df_join_org = df_join_org.loc[:, selected_cols]
        print("buo")

        table_name = "enr_buo"
        pk_columns = [BuoTarget.logsys.name, BuoTarget.budat.name]
        enr_columns = [BuoTarget.buo.name]
        update_columns = {
            BuoTarget.active_flag.name: False,
            BuoTarget.published_to.name: "'" + datetime.datetime.now().strftime("%Y-%m-%d") + "'",
        }
        insert_timestamp_column = BuoTarget.published_from.name
        active_flag_column = BuoTarget.active_flag.name
        print("buo")

        cdc = CDC(
            df_join_org,
            table_name,
            pk_columns,
            enr_columns,
            update_columns,
            insert_timestamp_column,
            active_flag_column,
        )
        print("buo")

        cdc()
        print("buo")

        return df_join_org
