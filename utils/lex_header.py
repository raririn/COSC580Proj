from enum import Enum, auto

class Token(Enum):

    def __generate_next_value__(name, start, count, last_values):
        return name

    def __str__(self):
        return lower(str(self.value))


class SQLKeyword(Token):

    # SQL Keywords
    Add = auto()
    AddConstraint = auto()
    Alter = auto()
    AlterColumn = auto() 
    AlterTable = auto() 
    All = auto() 
    And = auto() 
    Any = auto() 
    As = auto() 
    Asc = auto() 
    BackupDatabase = auto() 
    Between = auto() 
    Case = auto() 
    Check = auto() 
    Column = auto() 
    Constraint = auto() 
    Create = auto() 
    CreateDatabase = auto() 
    CreateIndex = auto() 
    CreateView = auto() 
    CreateTable = auto()
    CreateProcedure = auto() 
    CreateUniqueIndex = auto()
    Database = auto() 
    Default = auto()
    Delete = auto()
    Desc = auto()
    Distinct = auto()
    Drop = auto()
    DropColumn = auto()
    DropConstraint = auto()
    DropDatabase = auto()
    DropDefault = auto()
    DropIndex = auto()
    DropTable = auto()
    DropView = auto()
    Exec = auto()
    Exists = auto()
    ForeignKey = auto()
    From = auto()
    FullOuterJoin = auto()
    GroupBy = auto()
    Having = auto()
    In = auto()
    Index = auto()
    InnerJoin = auto()
    InsertInto = auto()
    IsNull = auto()
    IsNotNull = auto()
    Join = auto()
    LeftJoin = auto()
    Like = auto()
    LIMIT = auto()
    Not = auto()
    NotNull = auto()
    Or = auto()
    OrderBy = auto()
    OuterJoin = auto()
    PrimaryKey = auto()
    Procedure = auto()
    RightJoin = auto()
    RowNum = auto()
    Select = auto()
    SelectInto = auto()
    SelectTop = auto()
    Set = auto()
    Table = auto()
    Top = auto()
    TruncateTable = auto()
    Union = auto()
    UnionAll = auto()
    Unique = auto()
    Update = auto()
    Values = auto()
    View = auto()
    Where = auto()


class SQLFunction(Token):

    # SQL Functions
    Avg = auto()
    Count = auto()
    Max = auto()
    Min = auto()
    Sum = auto()


class SQLDataType(Token):

    # SQL Data types
    Char = auto()
    Date = auto()
    Double = auto()
    Float = auto()
    Int = auto()
    Varchar = auto()


class SQLOperator(Token):

    # Operators
    LT = '<'
    LE = '<='
    EQ = '=='
    NE = '!='
    GT = '>'
    GE = '>='

class SymbolType(Enum):

    DataType = auto() 
    MultipleKeyword = auto()
    Function = auto()
    Keyword = auto()
    Operator = auto()
    Number = auto()
    Identifier = auto()
    Delimiter = auto()

class Symbol(object):
    
    def __init__(self, name: str, token: Token, symbolType: SymbolType):
        self.name = name
        self.length = len(name)
        self.token = token
        self.type = symbolType
    




if __name__ == "__main__":
    print(list(map(lambda x: x.lower() ,[name for name, _ in Token.__members__.items()])))