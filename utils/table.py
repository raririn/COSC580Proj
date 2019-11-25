from utils.myexception import PrintException

class Table:
    def __init__(self, name, col_names = [], dtype = [], primary_key = None, tuples = {}):
        # WARNING: The constructor doesn't check the dimensions,
        # assuming everything passed in is correct!
        if isinstance(name, str):
            self.name = name
        else:
            raise Exception('')
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
    
    
    def _getNewName(self):
        for i in range(1000000000):
            yield str(i)

    def printall(self):
        print('Table %s' % self.name)
        print('Primary key: ' + str(self._primary_key))
        print(self._col_names)
        for k, v in self._tuples.items():
            print(k, v)
    
    @classmethod
    def newTable(cls, col_names, tuples, check = False):
        if check:
            # TODO:
            pass
        return cls(col_names = col_names, primary_key = None, tuples = tuples)

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

    def _select(self, col_names):
        
        pass

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