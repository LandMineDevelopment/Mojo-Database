from db_package.table import DBTable


def createTable() -> DBTable:
    alias val_range = 5
    alias tab_columns = 5
    alias tab_row_count = 5 
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)

    for col in range(tab_columns):
        for row in range(tab_row_count):
            tab[col*tab_row_count + row] = row%val_range
    return tab^

def main():
    print('Testing Table __eq__')
    lhs = createTable()
    rhs = createTable()
    print(lhs == rhs)