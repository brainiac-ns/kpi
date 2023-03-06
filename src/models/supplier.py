from columnnames import ColumnNames
from table import Table

class Supplier(Table):
    logsys = ColumnNames.logsys(primary_key=True, nullable=False)
    lifnr = ColumnNames.lifnr
    ifanr = ColumnNames.ifanr
    buo_org = ColumnNames.buo_org