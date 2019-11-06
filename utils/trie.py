class Trie:
    def __init__(self, terminate_symbol = None):
        self._d = {}
        if terminate_symbol:
            self.terminate_symbol = terminate_symbol
        else:
            self.terminate_symbol = '!'
    
    def insertList(self, words):
        words = sorted(words)
        for word in words:
            self.insert(word)        
    
    def insert(self, word):
        cur = self._d
        for w in word:
            if w not in cur:
                cur[w] = {}
            cur = cur[w]
        cur[self.terminate_symbol] == 1
    
    def search(self, word):
        cur = self._d
        for w in word:
            if w not in cur:
                return False
            cur = cur[w]
        return self.terminate_symbol in cur

    def startsWith(self, prefix):
        cur = self._d
        for w in prefix:
            if w not in cur:
                return False
            cur = cur[w]
        return True


if __name__ == "__main__":
    d = {}