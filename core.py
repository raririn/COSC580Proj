import utils.lex_header as lh
from utils.trie import Trie
from utils.utils import Param
from utils.myexception import PrintException
from src.table import Table
from src.db import DB
from myparser import Parser
from worker import Worker

from copy import deepcopy
import pickle
import time

class Core:

    DUMP_DUMPALL = 0

    def __init__(self):
        self.terminate_map = {}
        for k, v in Param.terminate_pair.items():
            self.terminate_map[v] = k
        #self.trie = self._constructTrie()
        #self.parser = Parser(self.trie)
        self.db = {}
        self.currentDB = 'defaultdb'
        self.tables = {}
        '''
        self.tables['Employee'] = <'Table' object at ...>
        self.tables['Company'] = <'Table' object at ...>
        '''

        self.parser = Parser()

        self.head_lines = 5
        self.printall = False
        self.return_time = False
    
    # W
    def _constructTrie(self):
        trie = Trie()
        # Written in multiple lines instead of loop to prevent dealing with hashing
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLKeyword.__members__.items()])), self.terminate_map['Keyword'])
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLFunction.__members__.items()])), self.terminate_map['Function'])
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLDataType.__members__.items()])), self.terminate_map['DataType'])
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLOperator.__members__.items()])), self.terminate_map['Operator'])
        return trie
    
    def _dump_db(self, file_path: str):
        new_db = DB(self.currentDB)
        new_db.tables = self.tables
        self.db[self.currentDB] = new_db
        f = open(file_path, 'wb')
        pickle.dump(self.db[self.currentDB], f)
        f.close()
        return 0

    def _load_db(self, file_path: str):
        with open(file_path, 'rb') as f:
            db_f = pickle.load(f)
        self.db[db_f.name] = db_f
        return 0
    
    def get_table(self, s: str):
        if s in self.tables:
            return self.tables[s]
        elif s.lower() in self.tables:
            return self.tables[s.lower()]
        elif s.upper() in self.tables:
            return self.tables[s.upper()]
        else:
            return False
    
    @staticmethod
    def get_table_from_dict(s:str, d: dict):
        if s in d:
            return d[s]
        elif s.lower() in d:
            return d[s.lower()]
        elif s.upper() in d:
            return d[s.upper()]
        else:
            return False
    
    def handler(self, s: str):
        if self.return_time:
            t = time.time()

        if ';' in s:
            s = s.split(';')
        else:
            s = [s]
        for i in s:
            try:
                d = self.parser.parse(i)
            except:
                PrintException.handlerError()
                return
            if d['type'] == 'select':
                if self.printall:
                    self.execute_select(d).formatout()
                else:
                    self.execute_select(d).head(self.head_lines)
            elif d['type'] == 'delete':
                self.execute_delete(d)
            elif d['type'] == 'insert':
                self.execute_insert(d)
            elif d['type'] == 'create_db':
                self.execute_create_db(d)
            elif d['type'] == 'drop_db':
                self.execute_drop_db(d)
            elif d['type'] == 'create_table':
                self.execute_create_table(d)
            elif d['type'] == 'drop_table':
                self.execute_drop_table(d)
            elif d['type'] == 'create_index':
                self.execute_create_index(d)
            elif d['type'] == 'drop_index':
                self.execute_drop_index(d)
            elif d['type'] == 'use_db':
                self.execute_use_db(d)
            else:
                PrintException.handlerError()
        if self.return_time:
            print('Executing queries takes %s seconds.' % (time.time() - t))       
    
    def execute_select(self, d: dict):
        '''
        d = {
            'query': {
                'select': {
                    'columns': ['a1', 'A2.c'],
                    'aggr_func': [],
                    'distinct': [],
                },
                'from': ['A1', 'A2', 'B1'],
                'where': {
                    'joins': [['A1.a', '=', 'A2.b'], ['A1.a', '>', 'B1.b']],
                    'conditions': [['AND', ['A1.a', '>', '2']], ['OR', ['B1.b', '=', '5']]]
                },
                'groupby': [],
                'orderby': [['A1.a'], True],
            },
            'tables': {
                'A1': 'T',
                'A2': 'T',
                'B1': 'K',
            },
            'columns': {
                'a1': 'A1.a',
                'A2.c': 'A2.c',
            },
        }
        '''
        query = d['query']
        columns = d['columns']
        alias_table = d['tables']
        
        # MODIFY THIS!!!
        runtime_dict = deepcopy(self.tables)
        # ^^^^
        
        for name, subquery in d['subquery'].items():
            runtime_dict[name] = self.execute_select(self.parser.parse_tokens(subquery))
        
        joins = query['where']['joins']
        conditions = query['where']['conditions']
        select = query['select']
        if len(query['groupby']) > 0:
            groupby = query['groupby']
        else:
            groupby = None
        if len(query['orderby']) > 0:
            orderby = query['orderby']
        else:
            orderby = None
        
        if len(joins) == 0:
            # If there is no join, we can assume there is only one table!
            target = d['tables'][query['from'][0]]
            final_table = self.get_table_from_dict(target, runtime_dict)
        
            #final_table.printall()

        for i in joins:
            '''
            Sample: i = ['A1.a', '=', 'A2.b']
            '''
            #print('\n', i, '\n')
            
            v1, operator, v2 = i[0].split('.'), i[1], i[2].split('.')
            t1 = alias_table[v1[0]]
            t2 = alias_table[v2[0]]
            flag_t1_unmodified = False
            flag_t2_unmodified = False
            
            if isinstance(t1, str):
                t1 = deepcopy(self.get_table_from_dict(t1, runtime_dict))
                flag_t1_unmodified = True
            if isinstance(t2, str):
                t2 = deepcopy(self.get_table_from_dict(t2, runtime_dict))
                flag_t2_unmodified = True
            
            if not flag_t1_unmodified:
                c1 = i[0]
            else:
                c1 = v1[1]
            
            if not flag_t2_unmodified:
                c2 = i[2]
            else:
                c2 = v2[1]        
            
            # Assign alias
            t1.name = v1[0]
            t2.name = v2[0]
            
            #print(t1.name, c1, t2.name, c2, v1, operator, v2)
            
            if flag_t1_unmodified and flag_t2_unmodified:
                ret_table = t1._join(t2, [c1, operator, c2], override_colname = Table.OVERRIDE_COLNAME_BOTH)
            elif (not flag_t1_unmodified) and flag_t2_unmodified:
                ret_table = t1._join(t2, [c1, operator, c2], override_colname = Table.OVERRIDE_COLNAME_LAST)
            elif flag_t1_unmodified and (not flag_t2_unmodified):
                ret_table = t1._join(t2, [c1, operator, c2], override_colname = Table.OVERRIDE_COLNAME_FIRST)
            else:
                ret_table = t1._join(t2, [c1, operator, c2], override_colname = Table.OVERRIDE_COLNAME_NONE)
            # Replace A1, A2 with A1 JOIN A2
            alias_table[v1[0]] = ret_table
            alias_table[v2[0]] = ret_table
            # Assume the last join will return the full joined table (?)
            final_table = ret_table
            
            #print(alias_table)
            #final_table.printall()
        
        if final_table == -1:	
            raise Exception('')
        T, cur_table = final_table, final_table
        #Sample: i = ['AND', ['A1.a', '>', '3']]
        for i in conditions:

            junction, condition = i[0], i[1]
            condition[2] = float(condition[2])
            if junction.upper() == 'AND':
                cur_table = cur_table._project(condition)
            else:
                new_t = T._project(condition)
                cur_table = cur_table._union(new_t)

        if select['aggr_func']:
            aggr_func = select['aggr_func'][0]
        else:
            aggr_func = select['aggr_func']

        if len(aggr_func) == 0:
            aggr_func = None
        else:
            aggr_func[1] = d['columns'][aggr_func[1]]
        distinct = select['distinct']
        if len(distinct) == 0:
            distinct = None 


        #cur_table.printall() 
        #print(columns, aggr_func)
        if select['columns'] == ['*']:
            columns = cur_table._col_names
        else:
            columns = [d['columns'][i] for i in select['columns']]

        if cur_table == -1:	
            raise Exception('')
        cur_table = cur_table._select(columns, distinct = distinct, aggr_func = aggr_func, orderby = orderby, groupby = groupby)
        reverse_columns_name_map = {}
        for k, v in d['columns'].items():
            reverse_columns_name_map[v] = k
        cur_table._col_names = list(map(lambda x: reverse_columns_name_map[x] if x in reverse_columns_name_map else x, cur_table._col_names))
        return cur_table
    
    def execute_delete(self, d):
        '''
        DELETE FROM Employees
        WHERE A = 1

        d = {
            'from': 'Employees',
            'where': ['A', '=', '1']
        }
        '''
        if self.get_table(d['from']):
            target = self.get_table(d['from'])
            if target._foreign_key:
                # A.a REFERENCES B.b
                # B._foreign_key = [['b', 'A', 'a', self.ONDELETE_CASCADE]]
                for fk in target._foreign_key:
                    target_c, ref_t, ref_c, fk_policy = fk[0], self.get_table(fk[1]), fk[2], fk[3]
                    if len(target_c) == 1:
                        target_c = target_c[0]
                    if len(ref_c) == 1:
                        ref_c = ref_c[0]
                    target_fk_loc = target._col_names.index(target_c)
                    ref_fk_loc = ref_t._col_names.index(ref_c)
                    to_d = target._delete(d['where'][0][1], try_d = True)
                    if fk_policy == Table.ONDELETE_NOACTION or fk_policy == Table.ONDELETE_RESTRICT:
                        for _, v in ref_t._tuples.items():
                            for j in to_d:
                                if v[ref_fk_loc] == j[target_fk_loc]:
                                    return -1
                    elif fk_policy == Table.ONDELETE_CASCADE:
                        del_list =[]
                        for k, v in ref_t._tuples.items():
                            for j in to_d:
                                if v[ref_fk_loc] == j[target_fk_loc]:
                                    del_list.append(k)
                        for i in del_list:
                            ref_t._tuples.pop(i)
                    elif fk_policy == Table.ONDELETE_SETNULL:
                        del_list =[]
                        for k, v in ref_t._tuples.items():
                            for j in to_d:
                                if v[ref_fk_loc] == j[target_fk_loc]:
                                    del_list.append(k)
                        for i in del_list:
                            ref_t._tuples[i][ref_fk_loc] = None
                target._delete(d['where'][0][1])  
            else:
                target._delete(d['where'][0][1])                            
        else:
            raise Exception('')
        return 0
    
    def execute_update(self, d):
        '''
        UPDATE Employees
        SET A = 1, B = 'a'
        WHERE C = 5

        d = {
            'update': 'Employees',
            'set': [['A', 1], ['B', 'a']]
            'where': ['C', '=', 5]
        }
        '''
        if self.get_table(d['update']):
            target = self.get_table(d['update'])
            target._update(d['set'], d['where'])
    
    def execute_insert(self, d):
        '''
        INSERT INTO Employees
        VALUES (1, 2, 3)

        d = {
            'insert_into': 'Employees',
            'values': [(1, 2, 3), (4, 5, 6)]
        }
        '''
        if self.get_table(d['insert_into']):
            target = self.get_table(d['insert_into'])
            vals = d['values']
            return target._insert(tuple(vals))
        else:
            raise Exception('')
        return 0

    def execute_create_db(self, d):
        '''
        CREATE DATABASE testdb;

        d = {
             'name': 'testdb',
        }
        '''
        if not d['name'] in self.db:
            self.db[d['name']] = DB(d['name'])
            return 0
        else:
            raise Exception('')
    
    def execute_use_db(self, d):
        '''
        USE testdb;

        d = {
             'name': 'testdb',
        }
        '''
        if d['name'] in self.db:
            if self.tables:
                # Save to old db
                self.db[self.currentDB].updateTable(self.tables)

            # Go to new db
            self.currentDB = d['name']
            self.tables = self.db[d['name']].tables
        else:
            raise Exception('')
        return 0
    
    def execute_drop_db(self, d):
        '''
        d = {
             'name': 'testdb',
        }
        '''
        if d['name'] in self.db:
            if d['name'] == self.currentDB:
                self.currentDB = None
                self.tables = {}
            self.db.pop(d['name'])
        else:
            raise Exception('')
        return 0

    def execute_create_index(self, d):
        '''
        {
            'name': index_name, 
            'table': table_name, 
            'columns': []
        }
        '''
        if self.get_table(d['table']):
            t = self.get_table(d['table'])
            for col in d['columns']:
                t._create_index(d['name'], col)
        else:
            raise Exception('')
        return 0

    def execute_drop_index(self, d):
        '''
        {
            'table': table_name, 
            'index': index_name
        }
        '''
        if self.get_table(d['table']):
            self.get_table(d['table'])._drop_index(d['index'])
        else:
            raise Exception('')
        return 0

    def execute_create_table(self, d):
        '''
        d = {
            'name': 'Employees',
            'col_names': ['A', 'B', 'C'],
            'dtype': ['int', 'varchar', 'date'],
            'primary_key': ['A'],
            'foreign_key': ['B', 'Company', 'A', 'CASCADE'],
        }

        If not specified, the last field in d['foreign_key'] can be None.
        '''
        fks = []
        for fk in d['foreign_key']:
            if len(fk) == 4:
                if not fk[3] or fk[3] == 'NOACTION':
                    fks.append([fk[0], fk[1], fk[2], Table.ONDELETE_NOACTION])
                elif fk[3] == 'CASCADE':
                    fks.append([fk[0], fk[1], fk[2], Table.ONDELETE_CASCADE])
                elif fk[3] == 'SETNULL':
                    fks.append([fk[0], fk[1], fk[2], Table.ONDELETE_SETNULL])
                else:
                    raise Exception
            else:
                raise Exception('')

        return self._create_table(d['name'], d['col_names'], d['dtype'], d['primary_key'], fks)
    
    def execute_drop_table(self, d):
        '''
        d = {
            'name': 'Employees',
            'if_exist': False,
        }
        '''
        return self._drop_table(d['name'], False)
    
    def _create_table(self, name: str, col_names: list, dtype: list, primary_key = None, foreign_key = None) -> int:
        '''
        e.g.
        CREATE TABLE Employees (
            A int,
            B varchar(255),
            C date,
            PRIMARY KEY (A),
            FOREIGN KEY (B) REFERENCES Company(A) ON DELETE CASCADE
        )

        is accepted as _create_table('Employees', ['A, 'B', 'C'], ['int', 'varchar', 'date'], primary key = 'A', foreign_key = ['B', 'Company', 'A', Table.ONDELETE_CASCADE]).

        If not specified, the on_delete parameter is set to Table.ONDELETE_NOACTION
        '''
        if name in self.tables:
            PrintException.keyError('_create_table')
            return -1
        if foreign_key:
            for key in foreign_key:
                target = key[1]
                fk = [key[2], name, key[0], key[3]]
                if target in self.tables:
                    self.get_table(target)._addForeignKeyConstraint(fk)
                else:
                    PrintException.keyError()
                    return -1

        self.tables[name] = Table.createTable(name, col_names = col_names, dtype = dtype, primary_key = primary_key)
        return 0
        
 
    def _drop_table(self, table_name, if_exist = False):
        if not self.get_table(table_name):
            if if_exist:
                return 0
            else:
                PrintException.keyError()
                return -1
        self.tables.pop(table_name)
        return 0


if __name__ == "__main__":
    c = Core()
