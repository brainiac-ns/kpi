import unittest
from unittest.mock import patch

import pandas as pd

from ifa.ifa import Ifa
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.enrichment_data.ifa_target import IfaTarget


class TestIfa(unittest.TestCase):
    @patch("base.enrichment.Enrichment.load_ifa_master")
    @patch("base.enrichment.Enrichment.load_supplier")
    @patch("base.enrichment.Enrichment.load_invoice")
    def test_ifa(self, mock_fuction_1, mock_fuction_2, mock_fuction_3):
        mock_fuction_1.return_value = self.mock_invoice()
        mock_fuction_2.return_value = self.mock_supplier()
        mock_fuction_3.return_value = self.mock_ifa_master()

        ifa = Ifa()

        df_lifnr = ifa.calculate_kpi(IfaInvoices.lifnr.name, IfaTarget.ifacmd_lifnr.name)
        df_krenr = ifa.calculate_kpi(IfaInvoices.krenr.name, IfaTarget.ifacmd_krenr.name)
        df_filkd = ifa.calculate_kpi(IfaInvoices.filkd.name, IfaTarget.ifacmd_filkd.name)

        self.assertListEqual(["ifanr1"], df_lifnr[IfaTarget.ifacmd_lifnr.name].tolist())
        self.assertListEqual(["#"], df_krenr[IfaTarget.ifacmd_krenr.name].tolist())
        self.assertListEqual(["ifanr1"], df_filkd[IfaTarget.ifacmd_filkd.name].tolist())

    def mock_invoice(self):
        data_invoice = {
            "logsys": ["logsys1"],
            "budat": ["2023-03-01"],
            "active_flag": ["active"],
            "lifnr": ["lifnr1"],
            "krenr": ["krenr"],
            "filkd": ["lifnr1"],
            "valid_org": ["valid_org1"],
            "published_from": ["2022-08-22"],
            "published_to": ["2022-12-22"],
        }

        df_invoice = pd.DataFrame(data_invoice)

        return df_invoice

    def mock_supplier(self):
        data_supplier = {
            "logsys": ["logsys1"],
            "lifnr": ["lifnr1"],
            "ifanr": ["ifanr1"],
            "valid_from": ["2022-10-13"],
            "valid_to": ["2023-02-22"],
            "buo_org": ["buo_org1"],
            "active_flag": ["active"],
            "published_from": ["2022-11-11"],
            "published_to": ["2022-12-22"],
        }

        df_supplier = pd.DataFrame(data_supplier)

        return df_supplier

    def mock_ifa_master(self):
        data_ifa_master = {
            "ifanr": ["ifanr1"],
            "loekz": [False],
            "deletion_date": ["2023-02-15"],
            "published_from": ["2023-01-01"],
            "published_to": ["2023-02-23"],
            "active_flag": ["active"],
        }

        df_ifa_master = pd.DataFrame(data_ifa_master)

        return df_ifa_master
