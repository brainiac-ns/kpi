from columnnames import ColumnNames
from table import Table

class IfaInvoices(Table):
    logsys = ColumnNames.logsys(primary_key=True, nullable=False)
    budat = ColumnNames.budat(primary_key=True, nullable=False)
    lifnr = ColumnNames.lifnr
    krenr = ColumnNames.krenr
    filkd = ColumnNames.filkd
    valid_org = ColumnNames.valid_org