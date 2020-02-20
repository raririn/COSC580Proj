from core import Core
from utils.myexception import PrintException


import signal
import sys

def sigint_handler(signal, frame):
    print('Interrupted')
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    c = Core()
    c.head_lines = 5
    c.return_time = True
    c.printall = False

    c._load_db('test.db')
    q = '''
        use defaultdb
        '''
    c.handler(q)
    

    print("Using default test db!")

    while True:
        print('>>> ', end = '')
        q = input()
        if q == "exit()":
            break
        c.handler(q)
