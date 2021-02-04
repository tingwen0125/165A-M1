# Global Setting for the Database
# PageSize, StartRID, etc..

'''
The config.py file is meant to act as centralized storage for all the configuration options
and the constant values used in the code. It is good practice to organize such
information into a Singleton object accessible from every file in the project. This class
will find more use when implementing persistence in the next milestone.
'''

PAGE_SIZE = 4096
INT_SIZE = 8
PAGE_RANGE_SIZE = 65536
MAX_NUM_RECORD = PAGE_SIZE / INT_SIZE
BASE_PAGE_PER_PAGE_RANGE = PAGE_RANGE_SIZE / PAGE_SIZE