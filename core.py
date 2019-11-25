import utils.lex_header as lh
from utils.trie import Trie
from utils.utils import Param
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
        self.tables = []
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



if __name__ == "__main__":
    c = Core()