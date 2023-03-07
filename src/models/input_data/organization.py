from src.models.base.column_names import ColumnNames
from table import Table

class Organization(Table):
    organization_id = ColumnNames.organization_id
    supplier_org = ColumnNames.supplier_org
    organization_type = ColumnNames.organization_type
    parent_hierarchy_ids = ColumnNames.parent_hierarchy_ids