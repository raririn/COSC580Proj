import utils.lex_header as lh
from utils.trie import Trie
from utils.utils import Param
from utils.myexception import PrintException
from src.table import Table
from myparser import Parser
from worker import Worker

import pickle

class Core:

    def __init__(self):
        self.terminate_map = {}
        for k, v in Param.terminate_pair.items():
            self.terminate_map[v] = k
        self.trie = self._constructTrie()
        self.parser = Parser(self.trie)
        self.tables = {}
        '''
        self.tables['Employee'] = <'Table' object at ...>
        self.tables['Company'] = <'Table' object at ...>
        '''

        # TODO: Consider if creating a run-time temporary table dict is necessary?
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

    def _run(self):
        pass

    def _select(self):
        pass

    def _project(self):
        pass

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
        if table_name in self.tables:
            PrintException.keyError()
            return -1
        if foreign_key and not foreign_key[3]:
            foreign_key = [foreign_key[0], foreign_key[1], foreign_key[2], Table.ONDELETE_NOACTION]
        if foreign_key:
            target = foreign_key[1]
            target._addForeignKeyConstraint(foreign_key)

        try:
            self.table[name] = Table.createTable(col_names = col_names, dtype = dtype, primary_key = primary_key)
            return 0
        except:
            return -1
        
        

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