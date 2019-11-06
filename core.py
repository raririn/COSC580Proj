from utils.lex_header import Token, Symbol
from utils.trie import Trie
from parser import Parser
from worker import Worker

class Core:

    def __init__(self):
        self.trie = Trie(tuple(map(lambda x: x.lower(), [name for name, _ in Token.__members__.items()])))
        self._run()
    
    def _constructTrie(self):


    def _run(self):
        pass



if __name__ == "__main__":
    pass