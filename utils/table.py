from utils.myexception import PrintException
import pickle

class Table:
    def __init__(self, name, col_names = [], dtype = [], primary_key = None, tuples = {}):
        # WARNING: The constructor doesn't check the dimensions,
        # assuming everything passed in is correct!
        self.name = name
        self._col_names = col_names
        self._dtype = dtype
        self._tuples = tuples
        '''
        list  (|col1 | col2 | col3 |)
        tuple (|val11| val12| val13|)
        tuple (|val21| val22| val23|)
        '''
        self._foreign_key = []
        '''
        [
            [column, reference, on_delete]
        ]
        '''
        self._index = None
        if primary_key:
            if primary_key not in self._col_names:
                PrintException.keyError()
            self._primary_key = primary_key
        else:
            self._primary_key = None
    
    @classmethod
    def createTable(cls, name: str, col_names: list, dtype: list, primary_key = None, tuples = {}):
        if (not name) or (not col_names) or (not dtype):
            return -1
        # Dimension check
        if len(col_names) != len(dtype):
            return -1
        # Type check
        if not isinstance(name, str) or not isinstance(col_names, list) or not isinstance(dtype, list):
            return -1
        return cls(name, col_names, dtype, primary_key, tuples)
    
    def dump(self):
        # TODO:
        pass

    def _getNewName(self):
        for i in range(1000000000):
            yield str(i)

    def printall(self):
        print('Table %s' % self.name)
        print('Primary key: ' + str(self._primary_key))
        print(self._col_names)
        for k, v in self._tuples.items():
            print(k, v)

    @property
    def defaultkey(self):
        return self.row+1

    @property
    def col(self):
        return len(self._col_names)
    
    @property
    def row(self):
        return len(self._tuples.items())

    def _checkdtype(self, newTuple: tuple) -> bool:
        for i in range(len(newTuple)):
            cur_dtype = self._dtype[i].lower()
            if cur_dtype == 'char' or cur_dtype == 'varchar':
                if not isinstance(newTuple[i], str):
                    return False
            elif cur_dtype == 'int':
                if not isinstance(newTuple[i], int):
                    return False
            elif cur_dtype == 'double' or cur_dtype == 'float':
                if not isinstance(newTuple[i], float):
                    return False
            # TODO: Handle Date: possibly via datetime.date
        return True

    #W
    def _insert(self, t):
        if self.col != len(t):
            PrintException.dimensionError()
            return -1
        if not self._checkdtype(t):
            PrintException.typeError()
            return -1
        if self._primary_key:
            primary_loc = self._col_names.index(self._primary_key)
            if t[primary_loc] in self._tuples:
                PrintException.keyError()
                return -1
            else:
                self._tuples[t[primary_loc]] = t
        else:
            self._tuples[self.defaultkey] = t
        return 0

    def _select(self, col_names, alias = None):
        # TODO: deal with primary key issues
        try:
            locs = [self._col_names.index(i) for i in col_names]
        except:
            PrintException.keyError()
            return -1
        ret_dtype = [self._dtype[i] for i in locs]
        ret = {}
        for k, v in self._tuples.items():
            ret[k] = tuple([v[i] for i in locs])
        if alias:
            return Table(name = alias, col_names = col_names, dtype = ret_dtype, primary_key = None, tuples = ret)
        else:
            return Table(name = self._getNewName(), col_names = col_names, dtype = ret_dtype, primary_key = None, tuples = ret)

    def _project(self, condition, alias = None):
        '''
        If there are multiple conditions, push them down and execute seperately.
        condition = | col_name | operator | value |
        '''
        if len(condition) < 2 or len(condition) > 4:
            raise Exception('')
        col_name, operator, val = condition[0], condition[1], condition[2]
        try:
            loc = self._col_names.index(col_name)
        except:
            PrintException.keyError()
            return -1
        ret_dtype = self._dtype[loc]
        ret = {}
        if operator == '>':
            for k, v in self._tuples.items():
                if v[loc] > val:
                    ret[k] = v
        elif operator == '=':
            for k, v in self._tuples.items():
                if v[loc] == val:
                    ret[k] = v            
        elif operator == '>=':
            for k, v in self._tuples.items():
                if v[loc] >= val:
                    ret[k] = v  
        elif operator == '<':
            for k, v in self._tuples.items():
                if v[loc] < val:
                    ret[k] = v
        elif operator == '<=':
            for k, v in self._tuples.items():
                if v[loc] <= val:
                    ret[k] = v                
        # TODO: return a new table
        if alias:
            return Table(alias, col_name, ret_dtype, None, ret)
        else:
            return Table(self._getNewName, col_name, ret_dtype, None, ret)

    def _join(self, other):
        pass