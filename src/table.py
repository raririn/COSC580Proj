from utils.myexception import PrintException

import pickle
from datetime import datetime

class Table:

    JOIN_MERGEJOIN = 0
    JOIN_NESTEDLOOP = 1

    ONDELETE_SETNULL = 0
    ONDELETE_CASCADE = 1
    ONDELETE_SETDEFAULT = 2
    ONDELETE_NOACTION = 3
    ONDELETE_RESTRICT = 4

    def __init__(self, col_names = [], dtype = [], primary_key = None, tuples = {}):
        # WARNING: The constructor doesn't check the dimensions,
        # assuming everything passed in is correct!
        self._col_names = col_names
        self._dtype = dtype

        # TODO:
        # Should we keep a name field inside tha table instance?
        
        # TODO: Consider constructing a new table from tuples. How to deal with keys?
        # 1. Keep the original keys (how to deal with collision?)
        # 2. Assign new keys anyway
        self._tuples = tuples
        '''
        list  (|col1 | col2 | col3 |)
        tuple (|val11| val12| val13|)
        tuple (|val21| val22| val23|)
        '''
        self._foreign_key = []
        '''
        [
            [column, reference->table, reference->col, on_delete]
            [...]
            ...
        ]
        e.g. TABLE A 
        FOREIGN KEY (a) REFERENCES B(b) ON DELETE CASCADE

        Then b._foreign_key = [['b', 'A', 'a', self.ONDELETE_CASCADE]]
        '''
        self._index = None
        if primary_key:
            if primary_key not in self._col_names:
                PrintException.keyError()
            self._primary_key = primary_key
        else:
            self._primary_key = None
    
    @classmethod
    def createTable(cls, col_names: list, dtype: list, primary_key = None, tuples = {}):
        if (not col_names) or (not dtype):
            return -1
        # Dimension check
        if len(col_names) != len(dtype):
            return -1
        # Type check
        if not isinstance(col_names, list) or not isinstance(dtype, list):
            return -1
        return cls(col_names, dtype, primary_key, tuples)
    
    def dump(self):
        # TODO:
        pass

    def _getNewName(self):
        for i in range(1000000000):
            yield str(i)

    def printall(self):
        print('Primary key: ' + str(self._primary_key))
        print([self._col_names[i] + ': ' + self._dtype[i] for i in range(self.col)])
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
            elif cur_dtype == 'date':
                if not isinstance(newTuple[i], datetime):
                    return False
        return True
    
    def _addForeignKeyConstraint(self, constraint: list) -> int:
        self._foreign_key.append(constraint)
        return 0

    #W
    def _insert(self, t: tuple) -> int:
        # TODO: Check constraints: like NOT NULL
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
    
    def _delete(self, condition):
        col, operator, val = condition[0], condition[1], condition[2]
        #TODO:
        pass

    def _update(self):
        #TODO:
        pass

    def _select(self, col_names, func = None, alias = None, orderby = None, groupby = None):
        # TODO: deal with primary key issues
        # Does the newly created table need to have primary keys?
        try:
            locs = [self._col_names.index(i) for i in col_names]
        except:
            PrintException.keyError()
            return -1
        ret_dtype = [self._dtype[i] for i in locs]
        ret = {}
        for k, v in self._tuples.items():
            ret[k] = tuple([v[i] for i in locs])
        
        if not func:
            func = ''

        if func.lower() == 'distinct':
            s = set()
            new_ret = {}
            for k, v in ret.items():
                if v in s:
                    pass
                else:
                    new_ret[k] = v
                    s.add(v)
            ret = new_ret
        elif func.lower() == 'average':
            new_ret = {}
            _sum = 0
            for k, v in ret.items():
                _sum += 1
            new_ret[0] = _sum / len(ret.items())
            ret = new_ret
        elif func.lower() == 'sum':
            new_ret = {}
            _sum = 0
            for k, v in ret.items():
                _sum += 1
            new_ret[0] = _sum
            ret = new_ret   
        elif func.lower() == 'count':
            new_ret = {}
            new_ret[0] = len(ret.items())
            ret = new_ret

        # TODO: How to deal with order by when the table is hashmap (i.e. unordered?)
        # Note that group by only exists with certain functions!
        if orderby:
            pass
        if groupby:
            pass         

        return Table(col_names = col_names, dtype = ret_dtype, primary_key = None, tuples = ret)

    def _project(self, condition, alias = None):
        '''
        If there are multiple conditions, push them down and execute seperately.
        condition = | col_name | operator | value |
        e.g. SELECT * FROM A WHERE A.a = 10
        condition ~ ['a', '=', 10]
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
        return Table(col_name, ret_dtype, None, ret)

    def _join(self, other, condition, mode = 1):
        '''
        Condition: | col_A | operator | col_B |
        e.g. Join A and B on key A.a = B.b
        Condition ~ ['a', '=', 'b']
        Note that the order matters.

        *: Assign new keys to the joined table.
        '''
        if mode == self.JOIN_NESTEDLOOP:
            count = 0
            ret = {}
            loc1 = self._col_names.index(condition[0])
            loc2 = other._col_names.index(condition[2])
            operator = condition[1]
            for _, v1 in self._tuples.items():
                for _, v2 in other._tuples.items():
                    if operator == '>':
                        if v1[loc1] > v2[loc2]:
                            ret[count] = v1 + v2
                            count += 1
                    elif operator == '=':
                        if v1[loc1] == v2[loc2]:
                            ret[count] = v1 + v2
                            count += 1                    
                    elif operator == '>=':
                        if v1[loc1] >= v2[loc2]:
                            ret[count] = v1 + v2
                            count += 1   
                    elif operator == '<=':
                        if v1[loc1] <= v2[loc2]:
                            ret[count] = v1 + v2
                            count += 1   
                    elif operator == '<':
                        if v1[loc1] < v2[loc2]:
                            ret[count] = v1 + v2
                            count += 1   
            # TODO: Consider primary key
            ret_col_name = self._col_names + other._col_names
            ret_dtype = self._dtype + other._dtype
            return Table(ret_col_name, ret_dtype, None, ret)
        elif mode == JOIN_MERGEJOIN:
            pass
        else:
            return -1