from utils.utils import Param
from utils.lex_header import Symbol
from utils.myexception import PrintException
from src.table import Table
import re

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

    def parse(self, string):
        tokens = re.split("\\s", string.strip())
        for i in range(len(tokens)):
            tokens[i] = tokens[i].lower()
        return self.parse_tokens(tokens)

    def parse_tokens(self, tokens):
        if tokens[0] == "select":
            res = {
                'type': 'select',
                'query': {
                    'select': {
                        'columns': [],
                        'aggr_func': [],
                        'distinct': []
                    },
                    'from': [],
                    'where': {
                        'joins': [],
                        'conditions': []
                    },
                    'groupby': [],
                    'orderby': []
                },
                'tables': {},
                'columns': {}
            }
            i = 1
            distinct = False
            if tokens[1] == "distinct":
                i += 1
                distinct = True
            ss = ''
            while i < len(tokens) and tokens[i] != "from":
                if re.match("^\\(.*", tokens[i]):
                    subquery = []
                    j = i
                    while not re.match("[^(]*\\)$", tokens[i]):
                        if i == j:
                            subquery.append(tokens[i][1:])
                        else:
                            subquery.append(tokens[i])
                        i += 1
                    if i == j:
                        subquery.append(tokens[i][1:-1])
                    else:
                        subquery.append(tokens[i][:-1])
                    i += 1
                    temp = self.evaluate(self.parse_tokens(subquery))
                    res['tables'][temp.name] = temp
                    ss += temp.name + ' '
                else:
                    ss += tokens[i] + ' '
                    i += 1
            
            cols = re.split(",", ss.strip())
            for col in cols:
                s = re.split("\\s", col.strip())
                if len(s) > 3 or len(s) < 1:
                    PrintException.syntaxError()
                    raise SyntaxError('')
                elif len(s) == 3:
                    if s[1] != "as":
                        PrintException.syntaxError()
                        raise SyntaxError('')
                    elif re.search("\\(", s[0]):
                        parts = re.split("[\\(\\)]", s[0])
                        res['query']['select']['aggr_func'].append([parts[0], s[2]])
                        res['query']['select']['columns'].append(s[2])
                        res['columns'][s[2]] = parts[1]
                        if distinct:
                            res['query']['select']['distinct'].append(s[2])
                    else:
                        res['query']['select']['columns'].append(s[2])
                        res['columns'][s[2]] = s[0]
                        if distinct:
                            res['query']['select']['distinct'].append(s[2])
                elif len(s) == 2:
                    if re.search("\\(", s[0]):
                        parts = re.split("[\\(\\)]", s[0])
                        res['query']['select']['aggr_func'].append([parts[0], s[1]])
                        res['query']['select']['columns'].append(s[1])
                        res['columns'][s[1]] = parts[1]
                        if distinct:
                            res['query']['select']['distinct'].append(s[1])
                    else:
                        res['query']['select']['columns'].append(s[1])
                        res['columns'][s[1]] = s[0]
                        if distinct:
                            res['query']['select']['distinct'].append(s[1])
                elif len(s) == 1:
                    if re.search("\\(", s[0]):
                        parts = re.split("[\\(\\)]", s[0])
                        res['query']['select']['aggr_func'].append([parts[0], parts[1]])
                        res['query']['select']['columns'].append(parts[1])
                        res['columns'][parts[1]] = parts[1]
                        if distinct:
                            res['query']['select']['distinct'].append(parts[1])
                    else:
                        res['query']['select']['columns'].append(s[0])
                        res['columns'][s[0]] = s[0]
                        if distinct:
                            res['query']['select']['distinct'].append(s[0])
            
            ss = ''
            i += 1
            while i < len(tokens) and tokens[i] != "where":
                if re.match("^\\(.*", tokens[i]):
                    subquery = []
                    j = i
                    while not re.match("[^(]*\\)$", tokens[i]):
                        if i == j:
                            subquery.append(tokens[i][1:])
                        else:
                            subquery.append(tokens[i])
                        i += 1
                    if i == j:
                        subquery.append(tokens[i][1:-1])
                    else:
                        subquery.append(tokens[i][:-1])
                    i += 1
                    temp = self.evaluate(self.parse_tokens(subquery))
                    res['tables'][temp.name] = temp
                    ss += temp.name + ' '
                else:
                    ss += tokens[i] + ' '
                    i += 1
            
            tables = re.split(",", ss.strip())
            for table in tables:
                s = re.split("\\s", table.strip())
                if len(s) > 3 or len(s) < 1:
                    PrintException.syntaxError()
                    raise SyntaxError('')
                elif len(s) == 3:
                    if s[1] != "as":
                        PrintException.syntaxError()
                        raise SyntaxError('')
                    else:
                        res['query']['from'].append(s[2])
                        res['tables'][s[2]] = s[0]
                elif len(s) == 2:
                    res['query']['from'].append(s[1])
                    res['tables'][s[1]] = s[0]
                elif len(s) == 1:
                    res['query']['from'].append(s[0])
                    res['tables'][s[0]] = s[0]
            
            i += 1
            last_junction = None
            while i < len(tokens) and tokens[i] != "order" and tokens[i] != "group":
                if tokens[i] in ['and', 'or']:
                    last_junction = tokens[i]
                    i += 1
                op0, op1, op2 = tokens[i], tokens[i + 1], tokens[i + 2]
                if Parser._is_val(op2):
                    if last_junction:
                        res['query']['where']['conditions'].append([last_junction, [op0, op1, op2]])
                    else:
                        res['query']['where']['conditions'].append(['and', [op0, op1, op2]])
                else:
                    res['query']['where']['joins'].append([op0, op1, op2])
                i += 3
            
            if i < len(tokens):
                if tokens[i] == "group":
                    i += 2
                    if i < len(tokens) and tokens[i] != "order":
                        ss = ''
                        while i < len(tokens) and tokens[i] != "order":
                            ss += tokens[i] + ' '
                            i += 1
                        parts = re.split(",", ss.strip())
                        for part in parts:
                            s = part.strip()
                            if len(s) > 0:
                                res['query']['groupby'].append(s)
                    else:
                        PrintException.syntaxError()
                        raise SyntaxError('')
                    i += 2
                    if i < len(tokens):
                        ss = ''
                        while i < len(tokens) and tokens[i] != 'group':
                            ss += tokens[i] + ' '
                            i += 1
                        parts = re.split(",", ss.strip())
                        for part in parts:
                            s = re.split("\\s", part.strip())
                            if len(s) > 2 or len(s) < 1:
                                PrintException.syntaxError()
                                raise SyntaxError('')
                            elif len(s) == 2:
                                if s[1] == 'asc' or s[1] == 'desc':
                                    res['query']['orderby'].append([s[0], s[1] == 'asc'])
                                else:
                                    PrintException.syntaxError()
                                    raise SyntaxError('')
                            elif len(s) == 1:
                                res['query']['orderby'].append([s[0], True])
                
                elif tokens[i] == "order":
                    i += 2
                    if i < len(tokens) and tokens[i] != 'group':
                        ss = ''
                        while i < len(tokens) and tokens[i] != 'group':
                            ss += tokens[i] + ' '
                            i += 1
                        parts = re.split(",", ss.strip())
                        for part in parts:
                            s = re.split("\\s", part.strip())
                            if len(s) > 2 or len(s) < 1:
                                PrintException.syntaxError()
                                raise SyntaxError('')
                            elif len(s) == 2:
                                if s[1] == 'asc' or s[1] == 'desc':
                                    res['query']['orderby'].append([s[0], s[1] == 'asc'])
                                else:
                                    PrintException.syntaxError()
                                    raise SyntaxError('')
                            elif len(s) == 1:
                                res['query']['orderby'].append([s[0], True])
                    else:
                        PrintException.syntaxError()
                        raise SyntaxError('')
                    i += 2
                    if i < len(tokens):
                        if tokens[i] != "order":
                            ss = ''
                            while i < len(tokens) and tokens[i] != "order":
                                ss += tokens[i] + ' '
                                i += 1
                            parts = re.split(",", ss.strip())
                            for part in parts:
                                s = part.strip()
                                if len(s) > 0:
                                    res['query']['groupby'].append(s)
                        else:
                            PrintException.syntaxError()
                            raise SyntaxError('')
                else:
                    PrintException.syntaxError()
                    raise SyntaxError('')

            return res

        elif tokens[0] == "create":
            if len(tokens) < 2:
                PrintException.syntaxError()
                raise SyntaxError('')
            elif tokens[1] == 'database':
                return {'type': 'create_db', 'name': tokens[2]}
            elif tokens[1] == 'table':
                res = {
                    'type': 'create_table', 
                    'name': tokens[2].strip().rstrip(')'),
                    'col_names': [],
                    'dtype': [],
                    'primary_key': [],
                    'foreign_key': []
                }
                if tokens[3] == 'as':
                    temp_table = self.evaluate(self.parse_tokens(tokens[4:]))
                else:
                    i = 3
                    ss = ''
                    while i < len(tokens) and tokens[i] != 'primary' and tokens[i] != 'foreign' and tokens[i] != ')':
                        ss += tokens[i] + ' '
                        i += 1
                    cols = re.split(",", ss.rstrip(' )').rstrip(' ,').lstrip(' ,()'))
                    for col in cols:
                        s = re.split("\\s", col.strip())
                        res['col_names'].append(s[0])
                        res['dtype'].append(s[1])
                    if i < len(tokens) and tokens[i] == 'primary':
                        if tokens[i + 1] != 'key':
                            PrintException.syntaxError()
                            raise SyntaxError('')
                        i += 2
                        ss = ''
                        while i < len(tokens) and tokens[i] != 'foreign':
                            ss += tokens[i] + ' '
                            i += 1
                        cols = re.split(",", ss.rstrip(' ,)').lstrip(' ,('))
                        for col in cols:
                            res['primary_key'].append(col.strip())
                    while i < len(tokens) and tokens[i] == 'foreign':
                        if tokens[i + 1] != 'key':
                            PrintException.syntaxError()
                            raise SyntaxError('')
                        i += 2
                        ss = ''
                        while i < len(tokens) and tokens[i] != 'references':
                            ss += tokens[i] + ' '
                            i += 1
                        cols = re.split(",", ss.rstrip(' )').lstrip(' ('))
                        self_cols = []
                        for col in cols:
                            self_cols.append(col.strip())
                        
                        i += 1
                        ss = ''
                        while i < len(tokens) and tokens[i] != 'on':
                            ss += tokens[i] + ' '
                            i += 1
                        table, cols = re.split("\\(", ss.rstrip(' )').strip())
                        foreign_cols = []
                        for col in re.split(",", cols.strip()):
                            foreign_cols.append(col.strip())
                        on_delete = 0
                        if i + 2 < len(tokens) and tokens[i] == 'on' and tokens[i + 1] == 'delete':
                            i += 2
                            if (i + 1) < len(tokens) and tokens[i] == 'set' and tokens[i + 1].rstrip(',)').strip() == 'null':
                                on_delete = Table.ONDELETE_SETNULL
                                i += 2
                            elif (i + 1) < len(tokens) and tokens[i] == 'set' and tokens[i + 1].rstrip(',)').strip() == 'default':
                                on_delete = Table.ONDELETE_SETDEFAULT
                                i += 2
                            elif (i + 1) < len(tokens) and tokens[i] == 'no' and tokens[i + 1].rstrip(',)').strip() == 'action':
                                on_delete = Table.ONDELETE_NOACTION
                                i += 2
                            elif i < len(tokens) and tokens[i].rstrip(',)').strip() == 'cascade':
                                on_delete = Table.ONDELETE_CASCADE
                                i += 1
                            elif i < len(tokens) and tokens[i].rstrip(',)').strip() == 'restrict':
                                on_delete = Table.ONDELETE_RESTRICT
                                i += 1
                            else:
                                PrintException.syntaxError()
                                raise SyntaxError('')
                        else:
                            PrintException.syntaxError()
                            raise SyntaxError('')
                        res['foreign_key'].append([self_cols, table, foreign_cols, on_delete])
                return res
            elif tokens[1] == 'index':
                index_name = tokens[2]
                if tokens[3] != 'on':
                    PrintException.syntaxError()
                    raise SyntaxError('')
                table_name = tokens[4]
                s = ''
                for i in range(5, len(tokens)):
                    s += tokens[i]
                parts = re.split(",", s)
                cols = []
                for part in parts:
                    cols.append(part.strip('()'))
                return {'type': 'create_index', 'name': index_name, 'table': table_name, 'columns': cols}
            else:
                PrintException.syntaxError()
                raise SyntaxError('')

        elif tokens[0] == "drop":
            if len(tokens) < 3:
                PrintException.syntaxError()
                raise SyntaxError('')
            elif tokens[1] == 'database':
                return {'type': 'drop_db', 'name': tokens[2]}
            elif tokens[1] == 'table':
                return {'type': 'drop_table', 'name': tokens[2]}
            elif tokens[1] == 'index':
                if len(tokens)< 5:
                    PrintException.syntaxError()
                    raise SyntaxError('')
                else:
                    return {'type': 'drop_index', 'table': tokens[4], 'index': tokens[2]}
            else:
                PrintException.syntaxError()
                raise SyntaxError('')
        
        elif tokens[0] == 'update':
            table = tokens[1]
            i = 3
            s = ''
            while i < len(tokens) and tokens[i] != 'where':
                s += tokens[i]
                i += 1
            parts = re.split(",", s)
            vals = []
            for part in parts:
                vals.append(re.split("=", part))
            i += 1
            conditions = []
            last_junction = None
            while i < len(tokens):
                if tokens[i] in ['and', 'or']:
                    last_junction = tokens[i]
                    i += 1
                op0, op1, op2 = tokens[i], tokens[i + 1], tokens[i + 2]
                if Parser._is_val(op2):
                    op2 = float(op2)
                if last_junction:
                    conditions.append([last_junction, [op0, op1, op2]])
                else:
                    conditions.append(['and', [op0, op1, op2]])
                i += 3
            return {
                'type': 'update',
                'update': table,
                'set': vals,
                'where': conditions
            }
        
        elif tokens[0] == 'insert':
            i = 4
            s = ''
            while i < len(tokens):
                s += tokens[i] + ' '
                i += 1
            s = s.lstrip('(').rstrip(')')
            ss = re.split(",", s)
            vals = []
            for t in ss:
                t = t.strip().lstrip('(').rstrip(')')
                if Parser._is_val(t):
                    vals.append(float(t))
                else:
                    vals.append(t)
            return {
                'type': 'insert',
                'insert_into': tokens[2],
                'values': vals
            }
        
        elif tokens[0] == 'delete':
            if len(tokens) < 7 or tokens[1] != 'from':
                PrintException.syntaxError()
                raise SyntaxError('')
            res = {
                'type': 'delete',
                'where': []
            }
            res['from'] = tokens[2]
            i = 4
            last_junction = None
            while i < len(tokens):
                if tokens[i] in ['and', 'or']:
                    last_junction = tokens[i]
                    i += 1
                op0, op1, op2 = tokens[i], tokens[i + 1], tokens[i + 2]
                if last_junction:
                    res['where'].append([last_junction, [op0, op1, op2]])
                else:
                    res['where'].append(['and', [op0, op1, op2]])
                i += 3
            return res

        elif tokens[0] == 'use':
            return {'type': 'use_db', 'name': tokens[1]}

        else:
            PrintException.syntaxError()
            raise SyntaxError('')

    @staticmethod
    def _is_val(x):
        try:
            float(x)
            return True
        except:
            return False
