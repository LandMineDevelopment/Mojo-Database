from db_package.table import DBTable

alias tab_row_count = 1_000_000 
alias tab_columns = 1_000
alias val_range = 20    
alias filter_cols = 20
alias filter_vals = 10

def createTable() -> DBTable:
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)

    for col in range(tab_columns):
        for row in range(tab_row_count):
            tab[col*tab_row_count + row] = row%val_range
    return tab^

def createFilterCols() -> List[Int]:
    var lst = List[Int]()
    for x in range(filter_cols):
        lst.append(x)
    return lst^

def createFilterVals() -> List[List[Int]]:
    var outer = List[List[Int]]()
    var inner: List[Int]
    for x in range(filter_cols):
        inner = List[Int]()
        for y in range(filter_vals):
            inner.append(y)
        outer.append(inner)
    return outer^


def main():
    var tab = createTable()
    var filter_cols = createFilterCols()
    var filter_vals = createFilterVals()
    # print(tab.filter(filter_cols,filter_vals))
    var filtered_tab = tab.filter(filter_cols,filter_vals)
    print('\nReturn Table')
    print('Column Count:',filtered_tab.numOfColumns, '--- Row Count:',filtered_tab.numOfRows)

    