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

class Core:

    DUMP_DUMPALL = 0

    def __init__(self):
        self.terminate_map = {}
        for k, v in Param.terminate_pair.items():
            self.terminate_map[v] = k
        #self.trie = self._constructTrie()
        #self.parser = Parser(self.trie)
        self.db = {}
        self.currentDB = None
        self.tables = {}
        '''
        self.tables['Employee'] = <'Table' object at ...>
        self.tables['Company'] = <'Table' object at ...>
        '''

        self._run()
    
    # W
    def _constructTrie(self):
        trie = Trie()
        # Written in multiple lines instead of loop to prevent dealing with hashing
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLKeyword.__members__.items()])), self.terminate_map['Keyword'])
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLFunction.__members__.items()])), self.terminate_map['Function'])
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLDataType.__members__.items()])), self.terminate_map['DataType'])
        trie.insertList(tuple(map(lambda x: x.lower(), [str(member) for name, member in lh.SQLOperator.__members__.items()])), self.terminate_map['Operator'])
        return trie
    
    def _dump_table(self, file_path: str, table_name = 0):
        if isinstance(table_name, str):
            table_name = [table_name]

        f = open(file_path, 'wb')
        if table_name == Core.DUMP_DUMPALL:
            pickle.dump(self.tables, f)
            f.close()
            return 0
        else:
            new_tableL = {}
            for i in table_name:
                new_tableL[i] = self.tables[i]
            pickle.dump(new_tableL, f)
            f.close()
            return 0
    
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
            final_table = runtime_dict[target]
        
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
                t1 = deepcopy(runtime_dict[t1])
                flag_t1_unmodified = True
            if isinstance(t2, str):
                t2 = deepcopy(runtime_dict[t2])
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
            if junction == 'AND':
                cur_table = cur_table._project(condition)
            else:
                new_t = T._project(condition)
                cur_table = cur_table._union(new_t)

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
        if columns == ['*']:
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
        if d['from'] in self.tables:
            target = self.tables[d['from']]
            if target._foreign_key:
                # A.a REFERENCES B.b
                # B._foreign_key = [['b', 'A', 'a', self.ONDELETE_CASCADE]]
                for fk in target._foreign_key:
                    target_c, ref_t, ref_c, fk_policy = fk[0], self.tables[fk[1]], fk[2], fk[3]
                    target_fk_loc = target._col_names.index(target_c)
                    ref_fk_loc = ref_t._col_names.index(ref_c)
                    to_d = target._delete(d['where'], try_d = True)
                    ref_t.printall()
                    if fk_policy == Table.ONDELETE_NOACTION or fk_policy == Table.ONDELETE_RESTRICT:
                        for _, v in ref_t._tuples.items():
                            for j in to_d:
                                if v[ref_fk_loc] == j[target_fk_loc]:
                                    return -1
                    elif fk_policy == Table.ONDELETE_CASCADE:
                        del_list =[]
                        for k, v in ref_t._tuples():
                            for j in to_d:
                                if v[ref_fk_loc] == j[target_fk_loc]:
                                    del_list.append(k)
                        for i in del_list:
                            ref_t._tuples.pop(i)
                    elif fk_policy == Table.ONDELETE_SETNULL:
                        del_list =[]
                        for k, v in ref_t._tuples():
                            for j in to_d:
                                if v[ref_fk_loc] == j[target_fk_loc]:
                                    del_list.append(k)
                        for i in del_list:
                            ref_t._tuples[i][ref_fk_loc] = None
            else:
                target._delete(d['where'])                            
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
        if d['update'] in self.tables:
            target = self.tables[d['update']]
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
        if d['insert_into'] in self.tables:
            target = self.tables[d['insert_into']]
            vals = d['values']
            for val in vals:
                target._insert(val)
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
        if d['table'] in self.tables:
            t = self.tables[d['table']]
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
        if d['table'] in self.tables:
            self.tables[d['table']]._drop_index(d['index'])
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
        if len(d['foreign_key']) == 4:
            if not d['foreign_key'][3] or d['foreign_key'][3] == 'NOACTION':
                fk = [d['foreign_key'][0], d['foreign_key'][1], d['foreign_key'][2], Table.ONDELETE_NOACTION]
            elif d['foreign_key'][3] == 'CASCADE':
                fk = [d['foreign_key'][0], d['foreign_key'][1], d['foreign_key'][2], Table.ONDELETE_CASCADE]
            elif d['foreign_key'][3] == 'SETNULL':
                fk = [d['foreign_key'][0], d['foreign_key'][1], d['foreign_key'][2], Table.ONDELETE_SETNULL]
            else:
                fk = d['foreign_key']
        else:
            fk = None
        return self._create_table(d['name'], d['col_names'], d['dtype'], d['primary_key'], fk)
    
    def execute_drop_table(self, d):
        '''
        d = {
            'name': 'Employees',
            'if_exist': False,
        }
        '''
        return self._drop_table(d['name'], d['if_exist'])
    
    def _load_table(self, file_path: str):
        pass

    def _run(self):
        self.parser = Parser()

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
        if foreign_key and not foreign_key[3]:
            foreign_key = [foreign_key[0], foreign_key[1], foreign_key[2], Table.ONDELETE_NOACTION]
        if foreign_key:
            target = foreign_key[1]
            fk = [foreign_key[2], name, foreign_key[0], foreign_key[3]]
            if target in self.tables:
                self.tables[target]._addForeignKeyConstraint(fk)
            else:
                PrintException.keyError()
                return -1

        self.tables[name] = Table.createTable(name, col_names = col_names, dtype = dtype, primary_key = primary_key)
        return 0
        
        

    def _create_index(self):
        pass

    def _drop_table(self, table_name, if_exist = False):
        if not table_name in self.tables:
            if if_exist:
                return 0
            else:
                PrintException.keyError()
                return -1
        self.tables.pop(table_name)
        return 0


    def _drop_index(self):
        pass


if __name__ == "__main__":
    c = Core()