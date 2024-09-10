from db_package.table import DBTable
from testing import assert_equal, assert_not_equal, assert_false, assert_raises, assert_true


alias val_range = 5
alias tab_columns = 5
alias tab_row_count = 5


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
