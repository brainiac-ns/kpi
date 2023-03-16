import json
import os
import shutil
import unittest

import pandas as pd

from models.input_data.supplier import Supplier
from src.preprocessing.preprocessing import PreprocessingJob


class TestPreprocessingJob(unittest.TestCase):
    def setUp(self):
        supplier1 = [
            {
                Supplier.active_flag.name: "active",
                Supplier.valid_from.name: "2023-01-01",
                Supplier.valid_to.name: "2023-02-02",
                Supplier.logsys.name: "logsys1",
                Supplier.lifnr.name: "lifnr1",
                Supplier.ifanr.name: "ifanr1",
                Supplier.buo_org.name: "buo_org1",
            }
        ]

        supplier2 = pd.DataFrame(
            {
                Supplier.active_flag.name: ["active"],
                Supplier.valid_from.name: ["2023-01-01"],
                Supplier.valid_to.name: ["2023-02-02"],
                Supplier.logsys.name: ["logsys2"],
                Supplier.lifnr.name: ["lifnr2"],
                Supplier.ifanr.name: ["ifanr2"],
                Supplier.buo_org.name: ["buo_org2"],
            }
        )

        os.mkdir("test-data")
        os.mkdir("test-data/supplier")

        with open("test-data/supplier/supplier.json", "w") as outfile:
            outfile.write(json.dumps(supplier1))
        print(supplier2)
        supplier2.to_csv("test-data/supplier/supplier.csv", index=False)

    def test_job_1(self):
        job1 = PreprocessingJob("tests/unit/test_config.yaml")
        job1()
        suppliers = pd.read_parquet("test-landing/supplier")
        self.assertEqual(2, len(os.listdir("test-landing/supplier")))
        self.assertEqual(2, len(suppliers))

    def tearDown(self):
        shutil.rmtree("test-data")
        shutil.rmtree("test-landing")
