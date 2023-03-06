from dataclasses import dataclass
from scmcolumn import SCMColumn

@dataclass
class ColumnNames:
    logsys = SCMColumn("logsys")
    budat = SCMColumn("budat")
    lifnr = SCMColumn("lifnr")
    krenr = SCMColumn("krenr")
    filkd = SCMColumn("filkd")
    valid_org = SCMColumn("valid_org")
    ifanr = SCMColumn("ifanr")
    buo_org = SCMColumn("buo_org")
    loekz = SCMColumn("loekz")
    deletion_date = SCMColumn("deletion_date")
    organization_id = SCMColumn("organization_id")
    supplier_org = SCMColumn("supplier_org")
    organization_type = SCMColumn("organization_type")
    parent_hierarchy_ids = SCMColumn("parent_hierarchy_ids")
    valid_from = SCMColumn("valid_from")
    valid_to = SCMColumn("valid_to")
    active_flag = SCMColumn("active_flag")