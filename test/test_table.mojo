from db_package.table import DBTable
from testing import assert_equal, assert_not_equal, assert_false, assert_raises, assert_true


alias tab_row_count = 5 
alias tab_columns = 5
alias val_range = 5    



# ===-------------------------------------------------------------------===#
# dunder: __eq__
# ===-------------------------------------------------------------------===#

def createTable() -> DBTable:
    var tab = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)

    for col in range(tab_columns):
        for row in range(tab_row_count):
            tab[col*tab_row_count + row] = row%val_range
    return tab

def test_eq():
    rhs = createTable()
    assert_equal(createTable(),rhs, msg = '__eq__ error: DBtables should match but do not')

# ===-------------------------------------------------------------------===#
# dunder: __ne__
# ===-------------------------------------------------------------------===#

def test_ne_index():
    rhs = createTable()
    rhs[0] = -1
    assert_not_equal(createTable(),rhs, msg = '__ne__ error: failed differing value at a given index')

def test_ne_cols():
    lhs = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)
    rhs = DBTable(numOfColumns = tab_columns+1, numOfRows = tab_row_count)
    assert_not_equal(createTable(),rhs, msg = '__ne__ error: column sizes do not match')

def test_ne_rows():
    lhs = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count)
    rhs = DBTable(numOfColumns = tab_columns, numOfRows = tab_row_count+1)
    assert_not_equal(createTable(),rhs, msg = '__ne__ error: row count do not match')

# ===-------------------------------------------------------------------===#
# method: filter
# ===-------------------------------------------------------------------===#
def createFilterCols(filter_cols: Int) -> List[Int]:
    var lst = List[Int]()
    for x in range(filter_cols):
        lst.append(x)
    return lst^

def createFilterVals(filter_cols: Int, filter_vals: Int) -> List[List[Int]]:
    var outer = List[List[Int]]()
    var inner: List[Int]
    for x in range(filter_cols):
        inner = List[Int]()
        for y in range(filter_vals):
            inner.append(y)
        outer.append(inner)
    return outer^

def test_filter():
    alias filter_cols = 1
    alias filter_vals = 1
    
    var tab = createTable()
    var filCols = createFilterCols(filter_cols)
    var filVals = createFilterVals(filter_cols, filter_vals)

    var resTab = tab.filter(filCols,filVals)
    
    # assert_equal(resTab.numOfRows, 1, msg = 'filter error: returns more/less rows than expected')
    # assert_equal(resTab.numOfColumns, tab.numOfColumns, msg = 'filter error: returns more/less columns than expected')
    # assert_equal(resTab[0,0], 0, msg = 'filter error: returns a row with value not in the filter set')

    var valSet: Set[Int]
    for col in filCols:
        valSet = filVals[col[]]
        for row in range(resTab.numOfRows):
            assert_true(resTab[col[],row] in valSet, 'filter error: returns a value not in the filter set')
