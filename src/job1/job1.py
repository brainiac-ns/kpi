import pandas as pd
import pyarrow as pa
from datetime import datetime

df_csv = pd.read_csv('../data/invoice/invoice.csv')
df_json = pd.read_json('../data/invoice/invoice.json')
df_union = pd.concat([df_csv, df_json])
df_union['budat'] = pd.to_datetime(df_union['budat'], format='%Y/%m/%d')
df_union['valid_from'] = pd.to_datetime(df_union['valid_from'], format='%Y/%m/%d')
df_union['valid_to'] = pd.to_datetime(df_union['valid_to'], format='%Y/%m/%d')

df_union.to_parquet('invoices', partition_cols=['budat'])

df_csv_supplier = pd.read_csv('../data/supplier/supplier.csv')
df_json_supplier = pd.read_json('../data/supplier/supplier.json')
df_union_supplier = pd.concat([df_csv_supplier, df_json_supplier])
df_union_supplier['valid_from'] = pd.to_datetime(df_union_supplier['valid_from'], format='%Y/%m/%d')
df_union_supplier['valid_to'] = pd.to_datetime(df_union_supplier['valid_to'], format='%Y/%m/%d')

df_union_supplier.to_parquet('suppliers', partition_cols=['logsys'])

df_csv_ifa_master = pd.read_csv('../data/ifa_master/ifa_master.csv')
df_json_ifa_master = pd.read_json('../data/ifa_master/ifa_master.json')
df_union_ifa_master = pd.concat([df_csv_ifa_master, df_json_ifa_master])
df_union_ifa_master['valid_from'] = pd.to_datetime(df_union_ifa_master['valid_from'], format='%Y/%m/%d')
df_union_ifa_master['valid_to'] = pd.to_datetime(df_union_ifa_master['valid_to'], format='%Y/%m/%d')

df_union_ifa_master.to_parquet('ifa_master', partition_cols=['ifanr'])

df_csv_organization = pd.read_csv('../data/organization/organization.csv')
df_csv_organization['parent_hierarchy_ids'] = df_csv_organization['parent_hierarchy_ids'].str.split(';')
df_json_organization = pd.read_json('../data/organization/organization.json')
df_union_organization = pd.concat([df_csv_organization, df_json_organization])
df_union_organization['valid_from'] = pd.to_datetime(df_union_organization['valid_from'], format='%Y/%m/%d')
df_union_organization['valid_to'] = pd.to_datetime(df_union_organization['valid_to'], format='%Y/%m/%d')

df_union_organization.to_parquet('organizations', partition_cols=['organization_type'])

pd.read_parquet('invoices')
pd.read_parquet('suppliers')
pd.read_parquet('ifa_master')
pd.read_parquet('organizations')