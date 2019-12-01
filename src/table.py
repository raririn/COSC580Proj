from utils.myexception import PrintException

import pickle
from datetime import datetime
import time

class Table:

    JOIN_MERGEJOIN = 0
    JOIN_NESTEDLOOP = 1
    JOIN_EXTERNALMERGE = 2

    OVERRIDE_COLNAME_NONE = 0
    OVERRIDE_COLNAME_FIRST = 1
    OVERRIDE_COLNAME_LAST = 2
    OVERRIDE_COLNAME_BOTH = 3

    ONDELETE_SETNULL = 0
    ONDELETE_CASCADE = 1
    ONDELETE_SETDEFAULT = 2
    ONDELETE_NOACTION = 3
    ONDELETE_RESTRICT = 4

    def __init__(self, name: str, col_names = None, dtype = None, primary_key = None, tuples = None):
        # WARNING: The constructor doesn't check the dimensions,
        # assuming everything passed in is correct!
        self.name = name
        if not col_names:
            col_names = []
        if not dtype:
            dtype = []
        if not tuples:
            tuples = {}
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
    def createTable(cls, name: str, col_names: list, dtype: list, primary_key = None, tuples = None):
        if (not col_names) or (not dtype):
            return -1
        # Dimension check
        if len(col_names) != len(dtype):
            return -1
        # Type check
        if not isinstance(col_names, list) or not isinstance(dtype, list):
            return -1
        return cls(name, col_names, dtype, primary_key, tuples)

    def _getNewName(self):
        for i in range(1000000000):
            yield str(i)

    def printall(self):
        print('Table <%s>' % self.name)
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
    
    def _delete(self, conditions: int) -> int:
        # TODO: Handle foreign keys
        col, operator, val = conditions[0], conditions[1], conditions[2]
        try:
            loc = self._col_names.index(col_name)
        except:
            PrintException.keyError()
            return -1
        del_list = []
        if operator == '>':
            for k, v in self._tuples.items():
                if v[loc] > val:
                    del_list.append(k)
        elif operator == '=':
            for k, v in self._tuples.items():
                if v[loc] == val:
                    del_list.append(k)          
        elif operator == '>=':
            for k, v in self._tuples.items():
                if v[loc] >= val:
                    del_list.append(k) 
        elif operator == '<':
            for k, v in self._tuples.items():
                if v[loc] < val:
                    del_list.append(k)
        elif operator == '<=':
            for k, v in self._tuples.items():
                if v[loc] <= val:
                    del_list.append(k)  
        for i in del_list:
            self._tuples.pop(i)
        return 0

    def _update(self, col_val_pairs: list, conditions: list) -> int:
        locs = [self._col_names.index(i) for i, _ in col_val_pairs]
        vals = [i for _, i in col_val_pairs]
        if operator == '>':
            for k, v in self._tuples.items():
                if v[loc] > val:
                    for x in len(locs):
                        v[locs[x]] = vals[x]
        elif operator == '=':
            for k, v in self._tuples.items():
                if v[loc] == val:
                    for x in len(locs):
                        v[locs[x]] = vals[x]       
        elif operator == '>=':
            for k, v in self._tuples.items():
                if v[loc] >= val:
                    for x in len(locs):
                        v[locs[x]] = vals[x]
        elif operator == '<':
            for k, v in self._tuples.items():
                if v[loc] < val:
                    for x in len(locs):
                        v[locs[x]] = vals[x]
        elif operator == '<=':
            for k, v in self._tuples.items():
                if v[loc] <= val:
                    for x in len(locs):
                        v[locs[x]] = vals[x]
        return 0

    def _select(self, col_names, distinct = None, aggr_func = None, alias = None, orderby = None, groupby = None):
        '''
        WARNING: the column that placed an aggregate function on should also be included in <col_names>

        func = None / ['avg', 'colA']
        distinct = ['colA', 'colB', ...]
        orderby = [['colA', 'colB', ...], desc]
        groupby = ['colA', 'colB', ...]
        '''
        ori_col_names = col_names
        try:
            ori_locs = [self._col_names.index(i) for i in col_names]
            if groupby:
                locs = [self._col_names.index(i) for i in set(groupby + col_names)]
            else:
                locs = ori_locs
        except:
            PrintException.keyError()
            return -1
        ret_dtype = [self._dtype[i] for i in locs]
        
        ret = {}
        for k, v in self._tuples.items():
            ret[k] = tuple([v[i] for i in locs])


        
        if not aggr_func or len(aggr_func) == 0:
            func = ['', ''] 
        else:
            func = aggr_func
        
        #TODO: Min, Max func

        if groupby:
            if func[0].lower() == 'avg':
                avg_loc = self._col_names.index(func[1])
                groupby_locs = [self._col_names.index(i) for i in groupby]
                new_ret = {}
                count_d = {}
                sum_d = {}
                for k, v in ret.items():
                    sum_d[tuple([v[i] for i in groupby_locs])] = sum_d.get(tuple([v[i] for i in groupby_locs]), 0) + v[avg_loc]
                    count_d[tuple([v[i] for i in groupby_locs])] = count_d.get(tuple([v[i] for i in groupby_locs]), 0) + 1
                for k, _ in sum_d.items():
                    sum_d[k] = sum_d[k] / count_d[k]
                for k, v in ret.items():
                     new_ret[tuple([v[i] for i in groupby_locs])] = (sum_d[tuple([v[i] for i in groupby_locs])], ) + v
                col_names = ['AVG(' + str(func[1]) +')'] + self._col_names
                ret_dtype = ['float'] + self._dtype
                ret = new_ret
            elif func[0].lower() == 'count':
                avg_loc = self._col_names.index(func[1])
                groupby_locs = [self._col_names.index(i) for i in groupby]
                new_ret = {}
                count_dict = {}
                for k, v in ret.items():
                    count_dict[tuple([v[i] for i in groupby_locs])] = count_dict.get(tuple([v[i] for i in groupby_locs]), 0) + 1
                    new_ret[tuple([v[i] for i in groupby_locs])] = (count_dict[tuple([v[i] for i in groupby_locs])], ) + v
                col_names = ['COUNT(' + str(func[1]) +')'] + self._col_names
                ret_dtype = ['int'] + self._dtype
                ret = new_ret
            elif func[0].lower() == 'sum':
                avg_loc = self._col_names.index(func[1])
                groupby_locs = [self._col_names.index(i) for i in groupby]
                new_ret = {}
                sum_d = {}
                for k, v in ret.items():
                    sum_d[tuple([v[i] for i in groupby_locs])] = sum_d.get(tuple([v[i] for i in groupby_locs]), 0) + v[avg_loc]
                    new_ret[tuple([v[i] for i in groupby_locs])] = (sum_d[tuple([v[i] for i in groupby_locs])], ) + v
                col_names = ['SUM(' + str(func[1]) +')'] + self._col_names
                ret_dtype = ['float'] + self._dtype
                ret = new_ret
            else:
                groupby_locs = [self._col_names.index(i) for i in groupby]
                new_ret = {}
                for k, v in ret.items():
                    new_ret[tuple([v[i] for i in groupby_locs])] = v
                ret = new_ret
        else:
            if func[0].lower() == 'avg':
                loc = self._col_names.index(func[1])
                new_ret = {}
                new_ret['AVG(' + str(func[1]) +')'] = 0
                count = 0
                for k, v in ret.items():
                    new_ret['AVG(' + str(func[1]) +')'] += v[loc]
                    count += 1
                new_ret['AVG(' + str(func[1]) +')'] /= count
                col_names = ['AVG(' + str(func[1]) +')']
                ret_dtype = ['float']
                ret = new_ret
            elif func[0].lower() == 'count':
                loc = self._col_names.index(func[1])
                new_ret = {}
                new_ret['COUNT(' + str(func[1]) +')'] = 0
                for k, v in ret.items():
                    new_ret['COUNT(' + str(func[1]) +')'] += 1
                col_names = ['COUNT(' + str(func[1]) +')']
                ret_dtype = ['int']
                ret = new_ret            
            elif func[0].lower() == 'sum':
                loc = self._col_names.index(func[1])
                new_ret = {}
                new_ret['SUM(' + str(func[1]) +')'] = 0
                for k, v in ret.items():
                    new_ret['SUM(' + str(func[1]) +')'] += v[loc]
                col_names = ['SUM(' + str(func[1]) +')']
                ret_dtype = ['int']
                ret = new_ret 

        if distinct:
            distinct_locs = [self._col_names.index(i) for i in distinct]
            s = set()
            new_ret = {}
            for k, v in ret.items():
                if tuple([v[i] for i in distinct_locs]) in s:
                    pass
                else:
                    new_ret[k] = tuple([v[i] for i in distinct_locs])
                    s.add(v)
            ret = new_ret

        # Note that groupby is prior to orderby!
        if orderby:
            locs, desc_flag = [self._col_names.index(i) for i in orderby[0]], orderby[1]
            if desc_flag:
                vals = sorted([v for _, v in ret.items()], key = lambda x: tuple([x[i] for i in locs]), reverse = True)
            else:
                vals = sorted([v for _, v in ret.items()], key = lambda x: tuple([x[i] for i in locs]), reverse = False)
            new_ret = {}
            count = 0
            for i in range(len(vals)):
                new_ret[count] = vals[i]
                count += 1
            ret = new_ret

        if func[0].lower() == 'first':
            firstkey = [k for k, _ in self._tuples.items()][0]
            new_ret = {}
            new_ret[firstkey] = ret[firstkey]
            ret = new_ret
        if func[0].lower() == 'last':
            lastkey = [k for k, _ in self._tuples.items()][-1]
            new_ret = {}
            new_ret[lastkey] = ret[lastkey]
            ret = new_ret
        
        if groupby and not aggr_func:
            for k, v in ret.items():
                ret[k] = tuple([v[i] for i in ori_locs])
        
        if groupby and aggr_func:
            for i in range(len(ori_col_names)):
                if ori_col_names[i] == aggr_func[1]:
                    old_name = ori_col_names
                    ori_col_names[i] = aggr_func[0].upper() + '(' + aggr_func[1] + ')'
            new_locs = [col_names.index(i) for i in ori_col_names]
            for k, v in ret.items():
                ret[k] = tuple(v[i] for i in new_locs)
            ret_dtype = [ret_dtype[col_names.index(i)] for i in ori_col_names]
            col_names = ori_col_names

        return Table(name = time.time(), col_names = col_names, dtype = ret_dtype, primary_key = None, tuples = ret)

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
        ret_dtype = self._dtype
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
        return Table(time.time(), self._col_names, ret_dtype, None, ret)

    def _join(self, other, condition, mode = 1, override_colname = 0):
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
            if override_colname == Table.OVERRIDE_COLNAME_NONE:
                ret_col_name = self._col_names + other._col_names
            elif override_colname == Table.OVERRIDE_COLNAME_FIRST:
                ret_col_name = list(map(lambda x: self.name + '.' + x, self._col_names)) + other._col_names
            elif override_colname == Table.OVERRIDE_COLNAME_LAST:
                ret_col_name = self._col_names + list(map(lambda x: other.name + '.' + x, other._col_names))
            elif override_colname == Table.OVERRIDE_COLNAME_BOTH:
                ret_col_name = list(map(lambda x: self.name + '.' + x, self._col_names)) + list(map(lambda x: other.name + '.' + x, other._col_names))
            ret_dtype = self._dtype + other._dtype
            return Table(time.time(), ret_col_name, ret_dtype, None, ret)
        elif mode == JOIN_MERGEJOIN:
            pass
        else:
            return -1
        
    def _union(self, other):
        s = set()
        ret = {}
        count = 0
        ret_col_name = self._col_names
        ret_dtype = self._dtype
        for _, v in self._tuples.items():
            if not v in s:
                ret[count] = v
                count += 1
        for _, v in other._tuples.items():
            if not v in s:
                ret[count] = v
                count += 1
        return Table(time.time(), ret_col_name, ret_dtype, None, ret)        
        
    def _index_join(self):
        pass