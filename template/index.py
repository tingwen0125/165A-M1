"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.

The Index class provides a data structure that allows fast processing of queries (e.g.,
select or update) by indexing columns of tables over their values. Given a certain
value for a column, the index should efficiently locate all records having that value. The
key column of all tables is usually indexed by default for performance reasons.
Supporting indexing is optional for this milestone. The API for this class exposes the
two functions create_index and drop_index (optional for this milestone).
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
