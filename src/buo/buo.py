from base.enrichment import Enrichment
from models.enrichment_data.ifa_invoices import IfaInvoices


class Buo(Enrichment):
    def __init__(self):
        super(Buo, self).__init__()
        self.df_invoice = self.load_invoice()
        self.df_supplier = self.load_supplier()
        self.df_ifa_master = self.load_ifa_master()  # check

    def __call__(self):
        df_join = self.merge_invoice_supplier(IfaInvoices.lifnr.name)

        print(df_join)


buo = Buo()
buo()
