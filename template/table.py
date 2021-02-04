from template.page import PageRange
from template.index import Index
from template.config import *
from time import time


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

'''
The Table class provides the core of our relational storage functionality. All columns are
64-bit integers in this implementation. Users mainly interact with tables through queries.
Tables provide a logical view over the actual physically stored data and mostly manage
the storage and retrieval of data. Each table is responsible for managing its pages and
requires an internal page directory that given a RID it returns the actual physical location
of the record. The table class should also manage the periodical merge of its
corresponding page ranges.
'''
#16 base pages in one page range
class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def getColumns(self):
        return self.columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.pageRanges = [PageRange(self.num_columns)]
        self.keyToBaseRID = {} 
        self.baseRID = 1
        self.tailRID = 1
        pass

    # Given a baseRID return a baseRecord
    # The way to access a value using a location: 
    # e.g. value = int.from_bytes(pageRanges[pageRange_index].basePageList
    # [basePageList_index].basePage[columnNum].data[offset_index*8:(offset_index+1)*8], 'big')

    def baseRIDToRecord(self, baseRID):
        pageRange_index = (baseRID-1) // 8192 #512*16
        basePageList_index = (baseRID-1-512 * 16 * pageRange_index) // 512
        offset_index = baseRID-512 * (16*pageRange_index+basePageList_index)-1
        baseRecord = []
        for i in range(4+self.num_columns):
            baseRecord.append(int.from_bytes(self.pageRanges[pageRange_index].basePageList[basePageList_index].basePage[i].data \
                [offset_index*8:(offset_index+1)*8], 'big'))
        return baseRecord
    
    def baseRIDToLocation(self, baseRID):
        pageRange_index = (baseRID-1) // 8192 #512*16
        basePageList_index = (baseRID-1-512 * 16 * pageRange_index) // 512
        offset_index = baseRID-512 * (16*pageRange_index+basePageList_index)-1
        location = [pageRange_index, basePageList_index, offset_index]
        return location
    '''
    def tailRIDToLocation(self, tailRID):
        pageRange_index = (tailRID-1) // 8192 
        tailPageList_index = (tailRID-1-512 * 16 * pageRange_index) // 512
        offset_index = (tailRID-512 * (16*pageRange_index+tailPageList_index)-1)
        location = [pageRange_index, tailPageList_index, offset_index]
    '''
    def writeByte(self, value, location, columnNum):
        pageRange_index = location[0]
        basePageList_index = location[1]
        offset_index = location[2]
        self.pageRanges[pageRange_index].basePageList[basePageList_index] \
            .basePage[columnNum].data[offset_index*8:(offset_index+1)*8] = \
                value.to_bytes(8, 'big')
        return True

    def printRecord(self, rid):
        pass

    '''
    def getPageR(self,rid): #given rid return the page range the rid record is at
        print("PageR", rid//MAX_NUM_RECORD//BASE_PAGE_PER_PAGE_RANGE)
        print(type(rid//MAX_NUM_RECORD//BASE_PAGE_PER_PAGE_RANGE))
        return int(rid//MAX_NUM_RECORD//BASE_PAGE_PER_PAGE_RANGE)
    '''

    def __merge(self):
        pass
 
