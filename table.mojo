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
        
    fn __str__(self) raises -> String:
        var printStr = str('---####------####---: \n')
        for col in range(self.numOfColumns):
            printStr += 'col_'+str(col)+': ['
            for val in range(self.numOfRows):
                printStr += str(self.__getitem__(col,val))+ ', '
            printStr += ']\n'
        return printStr + '---############---\n'
    
    # ===-------------------------------------------------------------------===#
    # Methods
    # ===-------------------------------------------------------------------===#

    fn addEmptyCol(inout self, numberOfNewCols: Int):
        for col in range(numberOfNewCols):
            self.table.append(0)

    fn addCol(inout self, owned *columns: List[Int]) raises:
        for col in columns:
            if len(col[]) >= self.numOfRows:
                raise 'When adding columns the number of elements must match the number of rows in the table'
            for val in col[]:
                self.table.append(val[])
    
    fn type(self):
        print('DBTable')      
    
