from models.base.column_names import ColumnNames
from models.base.table import Table


class Supplier(Table):
    logsys = ColumnNames.logsys(primary_key=True, nullable=False)
    lifnr = ColumnNames.lifnr
    ifanr = ColumnNames.ifanr
    buo_org = ColumnNames.buo_org

    def __iter__(self):
        return iter(
            [self.logsys, self.lifnr, self.ifanr, self.valid_from, self.valid_to, self.buo_org, self.active_flag]
        )
