from utils.utils import Param
from utils.lex_header import Symbol

class Parser:

    def __init__(self, trie, terminate_map = None):
        self.trie = trie
    
    def _tokenize(self, word):
        if self.trie.search(word):
            terminate = Param.terminate_pair[self.trie.search(word)]
        else:
            if word in Param.punctuation:
                terminate  = 'Delimiter'
            else:
                try:
                    int(word)
                    terminate = 'Value'
                except:
                    terminate = 'Identifier'
        return Symbol(word, terminate)

    def _split(self, string):
        parts = string.split()

        # TODO: Handle multiple keywords

        return list(map(self._tokenize, parts))