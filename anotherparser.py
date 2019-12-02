from utils.utils import Param
from utils.lex_header import Symbol
from utils.myexception import PrintException
from src.table import Table
import re

def itertoken(tokens):
    for i in tokens:
        yield i


class Parser:

    def __init__(self):
        pass
    


    def parse(string):
        tokens = re.split("\\s", string.strip())
