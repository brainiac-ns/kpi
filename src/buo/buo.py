import pandas as pd
from sqlalchemy import create_engine

from base.enrichment import Enrichment
from constants import Constants
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
        df_join = self.merge_invoice_supplier(IfaInvoices.lifnr.name)

        max_idx = self.df_organization.groupby(Organization.supplier_org.name)[
            Organization.published_from.name
        ].idxmax()
        self.df_organization = self.df_organization.loc[max_idx]

        df_join_org = pd.merge(
            df_join,
            self.df_organization,
            how="left",
            left_on=Supplier.buo_org.name,
            right_on=Organization.supplier_org.name,
        )

        df_join_org[Constants.BUO.value] = Constants.DUMMY_BUO.value
        df_join_org.loc[df_join_org[Organization.organization_id.name].notna(), Constants.BUO.value] = df_join_org[
            Organization.organization_id.name
        ]

        df_join_org[Organization.parent_hierarchy_ids.name] = df_join_org[Organization.parent_hierarchy_ids.name].apply(
            lambda x: str(x)
        )

        engine = create_engine("postgresql://user:admin@localhost:54320/postgres")

        df_join_org.to_sql(name="enr_buo", con=engine, if_exists="replace", index=False)

        return df_join_org


buo = Buo()
buo()
