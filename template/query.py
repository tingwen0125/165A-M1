from template.table import Table, Record
from template.index import Index
from template.page import Page, BasePage, PageRange
import datetime
'''
The Query class provides standard SQL operations such as insert, select,
update, delete and sum. The select function returns the specified set of columns
from the record with the given key (if available). The insert function will insert a new
record in the table. All columns should be passed a non-NULL value when inserting. The
update function updates values for the specified set of columns. The delete function
will delete the record with the specified key from the table. The sum function will sum
over the values of the selected column for a range of records specified by their key
values. We query tables by direct function calls rather than parsing SQL queries.
'''

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified key
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):

        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        '''record example:[0, 0, 20210131111207, 0, 906659671, 93, 0, 0, 0]'''
        # Check if key is duplicated
        if (columns[self.table.key] in self.table.keyToBaseRID):
            return False
        total_col = []
        schema_encoding = int('0' * self.table.num_columns, 2)
        time = datetime.datetime.now()
        int_time = int(time.strftime("%Y%m%d%H%M%S"))
        curPageRange = self.table.pageRanges[-1]
        curBasePage = curPageRange.basePageList[-1]

        # open a new page range or new base page
        if curPageRange.has_capacity() == False:
            self.table.pageRanges.append(PageRange(self.table.num_columns))
            curPageRange = self.table.pageRanges[-1]
            curBasePage = curPageRange.basePageList[-1]
        elif curBasePage.has_capacity() == False:
            curPageRange.basePageList.append(BasePage(self.table.num_columns))
            curBasePage = curPageRange.basePageList[-1]
  
        total_col.extend([0, self.table.baseRID, int_time, schema_encoding])
        total_col += columns
        for i in range(len(total_col)):
            curBasePage.basePage[i].write(total_col[i])
            #test
            #start = (curBasePage.basePage[i].num_records - 1) * 8
            #end = curBasePage.basePage[i].num_records * 8
            #int_val=int.from_bytes(curBasePage.basePage[i].data[start:end],'big')
            #print(int_val)
        
        self.table.keyToBaseRID[total_col[self.table.key + 4]] = self.table.baseRID
        self.table.baseRID += 1
        return True
    
    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        listSelect = []
        recordSelect = []

        #locate record position
        if key in self.table.keyToBaseRID.keys():
            baseRID = self.table.keyToBaseRID[key]
            baseRecord = self.table.baseRIDToRecord(baseRID)

        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                val = baseRecord[i+4]
                recordSelect.append(val)
            else:
                recordSelect.append(None)
        listSelect.append(Record(baseRID, key, recordSelect))
        return listSelect

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    '''
    def getUpdateRID(self,key): 
        return self.table.keyToBaseRID[key]

    def getUpdatePageR(self,rid):
        return self.table.getPageR(rid)
    '''

    def update(self, key, *columns):
        baseRID = self.table.keyToBaseRID[key]
        location = self.table.baseRIDToLocation(baseRID)
        pageRange_index = location[0]
        baseRecord = self.table.baseRIDToRecord(baseRID)
        #print("Before update:", baseRecord)

        #check if the tail page in that page range still have space
        if self.table.pageRanges[pageRange_index].tailPageList[-1].has_capacity() == False: #if no capacity, add a new tail page
            self.table.pageRanges[pageRange_index].tailPageList.append(BasePage(self.table.num_columns)) 
        updateEncoding = ""  #updated schema encoding
        for i in range(len(columns)):
            if columns[i] == None:
                updateEncoding += "0"
            else:
                updateEncoding += "1"
        
        updateEncoding = int(updateEncoding, 2)
        time = datetime.datetime.now()
        int_time = int(time.strftime("%Y%m%d%H%M%S"))

        baseRecordIndirect = baseRecord[0]
        tailIndirect = 0
        # Current tailRecord is not the first update to a baseRecord then get the last tail record RID
        if (baseRecordIndirect != 0):
            tailIndirect = baseRecordIndirect

        # Update baseRecord indirect column
        self.table.writeByte(self.table.tailRID, location, 0)
            
        tailrecord = [self.table.tailRID, tailIndirect, int_time,updateEncoding]+list(columns)
        currTailPage = self.table.pageRanges[pageRange_index].tailPageList[-1]
        for i in range(len(tailrecord)):
            currTailPage.basePage[i].write(tailrecord[i])
        self.table.tailRID += 1

        #baseRecord = self.table.baseRIDToRecord(baseRID)
        #print("After update:", baseRecord)
        return True
        
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        startRID = start_range + 1
        endRID = end_range + 1
        sum = 0
        if (startRID > self.table.baseRID):
            return False
        for i in range(startRID, endRID):
            baseRecord = self.table.baseRIDToRecord(i)
            sum += baseRecord[aggregate_column_index+4]
        return sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

