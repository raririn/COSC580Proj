from exception import PrintException

class Table:
    def __init__(self, col_names = [], primary_key = None):
        self._col_names = col_names
        self._tuples = {}
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
        self._primary_key = primary_key
    
    @classmethod
    def constructTable(cls):
        pass

    @property
    def defaultkey(self):
        return self.row+1

    @property
    def col(self):
        return len(self.col_names)
    
    @property
    def row(self):
        return len(self.tuples.items())

    def _insert(self, t):
        if self.col != len(t):
            PrintException.dimensionError()
            return -1
        if self._primary_key:
            pass
        else:
            self.tuples[self.defaultkey] = t
        return 0

    def _select(self, col_names):
        
        pass

    def _project(self, condition):
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
        ret = {}
        if operator == '>':
            for k, v in self._tuples.items():
                if i[loc] > val:
                    ret[k] = v
        elif operator == '=':
            for k, v in self._tuples.items():
                if i[loc] = val:
                    ret[k] = v            
        elif operator == '>=':
            for k, v in self._tuples.items():
                if i[loc] >= val:
                    ret[k] = v  
        elif operator == '<':
            for k, v in self._tuples.items():
                if i[loc] < val:
                    ret[k] = v
        elif operator == '<=':
            for k, v in self._tuples.items():
                if i[loc] <= val:
                    ret[k] = v                
        # TODO: return a new table

    def _join(self, other):
        pass