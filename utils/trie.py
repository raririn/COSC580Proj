class Trie:
    def __init__(self):
        self._d = {}
        self.terminate_symbols = []
    
    def __str__(self):
        return str(self._d)
    
    def insertList(self, words, terminate_symbol = '!'):
        if not terminate_symbol in self.terminate_symbols:
            self.terminate_symbols.append(terminate_symbol)
        for word in words:
            self.insert(word, terminate_symbol)        
    
    def insert(self, word, terminate_symbol = '!'):
        word = word.lower()
        if not terminate_symbol in self.terminate_symbols:
            self.terminate_symbols.append(terminate_symbol)
        cur = self._d
        for w in word:
            if w not in cur:
                cur[w] = {}
            cur = cur[w]
        cur[terminate_symbol] = 1
    
    def search(self, word):
        word = word.lower()
        cur = self._d
        for w in word:
            if w not in cur:
                return False
            cur = cur[w]
        for i in self.terminate_symbols:
            if i in cur:
                return i
        else:
            return False

    def startsWith(self, prefix):
        prefix = prefix.lower()
        cur = self._d
        for w in prefix:
            if w not in cur:
                return False
            cur = cur[w]
        return True


if __name__ == "__main__":
    d = {}