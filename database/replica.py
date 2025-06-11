from database.postgres import PostgresDatabaseConnector
from extend.extend import ExtendAlgorithm

class Replica:
    def __init__(self, id, hostname, port = 5432, dbname = 'tpchdb', user = 'sam', password = ''):
        self.id = id
        self.hostname = hostname
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = PostgresDatabaseConnector(dbname, self.connection_string())
        self.algorithm = None

    def connection_string(self):
        return f'host={self.hostname} port={self.port} dbname={self.dbname} user={self.user} password={self.password}'
    
    def connector(self):
        return self.conn
    
    def create_extend_algorithm(self, config = None):
        self.algorithm = ExtendAlgorithm(self.conn, config)
