class DB:

    def __init__(self, name):
        self.name = name
        self.tables = {}

    def updateTable(self, table: dict):
        self.tables = table
    