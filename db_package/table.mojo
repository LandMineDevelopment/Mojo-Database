from collections import List
from collections.dict import Dict, _DictKeyIter, _DictValueIter, _DictEntryIter




@value
struct Lookup[Orig: KeyElement]:
    var columnName: String
    var origToIndex: Dict[Orig,Int]
    var indexToOrig: Dict[Int,Orig]
    var nextIndex: Int

    fn __init__(inout self,columnName: String):
        self.columnName = columnName
        self.origToIndex = Dict[Orig,Int]()
        self.indexToOrig =  Dict[Int,Orig]()
        self.nextIndex = 0 

    fn encode(inout self, origs: List[Orig]):
        for val in origs:
            self.encode(val[])
    
    fn encode(inout self, val: Orig):
        if val not in self.origToIndex:
            self.origToIndex[val] = self.nextIndex
            self.indexToOrig[self.nextIndex] = val
            self.nextIndex += 1

    fn decode(self, ints: List[Int]) raises -> List[Orig]:
        var retOrigs = List[Orig]()
        for i in ints:
            retOrigs.append(self.decode(i[]))
        return retOrigs
    
    fn decode(self, ints: Int) raises -> Orig:
        if ints not in self.indexToOrig:
            raise('int not in lookup')
        return self.indexToOrig[ints]

@value 
struct DBTable:
    var table: List[Int]
    var numOfColumns: Int
    var numOfRows: Int

    # ===-------------------------------------------------------------------===#
    # Life cycle method
    # ===-------------------------------------------------------------------===#

    fn __init__(inout self, numOfColumns: Int, numOfRows: Int):
        self.table = List[Int](capacity = numOfColumns*numOfRows)
        self.numOfColumns = numOfColumns
        self.numOfRows = numOfRows

    # ===-------------------------------------------------------------------===#
    # Dunders
    # ===-------------------------------------------------------------------===#

    fn __getitem__(self, index: Int) raises -> Int:
        if index >= self.numOfRows * self.numOfColumns:
            raise 'index out of bounds'
        
        return self.table[index]

    fn __getitem__(self, col: Int, row: Int) raises -> Int:
        if col >= self.numOfColumns:
            raise 'column index out of bounds'
        if row >= self.numOfRows:
            raise 'row index out of bounds'
        
        return self.table[col*self.numOfRows + row]

    fn __setitem__(inout self, index: Int, val: Int) raises:
        if index >= self.numOfRows * self.numOfColumns:
            raise 'index out of bounds'
        
        self.table[index] = val

    fn __setitem__(inout self, col: Int, row: Int, val: Int) raises:
        if col >= self.numOfColumns:
            raise 'column index out of bounds'
        if row >= self.numOfRows:
            raise 'row index out of bounds'
        
        self.table[col*self.numOfRows + row] = val
        
    fn __str__(self) -> String:
        try:
            var printStr = str('---####------####---: \n')
            for col in range(self.numOfColumns):
                printStr += 'col_'+str(col)+': ['
                for val in range(self.numOfRows):
                    printStr += str(self.__getitem__(col,val))+ ', '
                printStr += ']\n'
            return printStr + '---############---\n'
        except:
            return 'issue with __str__, probably out of bounds issue'
    
    fn __eq__(self, rhs: Self) -> Bool:
        if self.numOfColumns != rhs.numOfColumns or self.numOfRows != rhs.numOfRows:
            return False
        for ind in range(self.numOfColumns*self.numOfRows):
            if self.table[ind] != rhs.table[ind]:
                return False
        return True

    fn __ne__(self, rhs: Self) -> Bool:
        if self.numOfColumns != rhs.numOfColumns or self.numOfRows != rhs.numOfRows:
            return True
        for ind in range(self.numOfColumns*self.numOfRows):
            if self.table[ind] != rhs.table[ind]:
                return True
        return False
    
    # ===-------------------------------------------------------------------===#
    # Methods: update/write
    # ===-------------------------------------------------------------------===#

    fn addEmptyCol(inout self, numberOfNewCols: Int):
        for col in range(numberOfNewCols):
            self.table.append(-1)

    fn addCol(inout self, owned *columns: List[Int]) raises:
        for col in columns:
            if len(col[]) >= self.numOfRows:
                raise 'When adding columns the number of elements must match the number of rows in the table'
            for val in col[]:
                self.table.append(val[])
    
    fn type(self):
        print('DBTable')      

    # ===-------------------------------------------------------------------===#
    # Methods: read
    # ===-------------------------------------------------------------------===#
    '''
    fn tableFilter(self, columnNames: List[Int], columnValues: List[List[Int]]) raises -> DBTable:
        var filter_column_len = len(columnNames)
        var resSet = Set[Int]()
        var numOfColumns = self.numOfColumns
        var numOfRows = self.numOfRows

        var start = now()
        var valSet = Set[Int](columnValues[0])

        #rows for the first column
        # for row in range(numOfRows):
        #     if tab[row] in valSet:
        #         resSet.add(row)
        # print('get rowlist from first column')    
        # print(str((now()-start)/1_000_000_000)+'sec')

        start = now()
        # var colAve = 0.0
        # var colTime = now()

        # for row in resSet:
        #     for col in range(1, filter_column_len):
        #         colTime = now()
        #         valSet = Set[Int](columnValues[col])
        #         if tab[col,row[]] not in valSet:
        #             resSet.remove(row[])
        #             colAve += (now()-colTime)/1_000_000_000
        #             break
        #         colAve += (now()-colTime)/1_000_000_000
        # print('colAve:')
        # print(str(colAve/(filter_column_len-1))+'sec')
        var hashed = 0
        var checkSet = Set[Int]()
        for l in range(len(columnValues)):
            hashed = hash(l)
            for elem in columnValues[l]:
                hashed = hash(hashed+elem[])
                checkSet.add(hashed)

        
        
        var commonRow = True 
        for row in range(numOfRows):
            commonRow = True 
            for col in range(filter_column_len):
                
                # if tab[col,row] not in columnValues[col]:
                if hash(hash(col)+self.table[col,row]) in checkSet:
                    commonRow = False
                    break
            if commonRow:
                resSet.add(row)

        print('combine OR vals & find common rows')    
        print(str((now()-start)/1_000_000_000)+'sec')

        start = now()
        var newNumOfRows = len(resSet)
        var retTab = DBTable[Int](numOfColumns = numOfColumns, numOfRows = newNumOfRows)
        # retTab.table.resize(numOfColumns*newNumOfRows, 0)
        
        for col in range(numOfColumns):
            for row in resSet:
                retTab.append(tab[col, row[]])
                # retTab[col,row[]] = tab[col, row[]]

        print('create ret tab')    
        print(str((now()-start)/1_000_000_000)+'sec')
        return retTab
    '''

