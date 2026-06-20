from src.database.mysql_client import MySQLDatabase
from src.database.tables import table_staging 

# conexión a base de datos
#db = MySQLDatabase("financialmarkets")


class CreateTables(MySQLDatabase):
    
    def __init__(self, database):
        super().__init__(database)
    
    # creamos la tabla staging
    def staging(self, table_name):
        query = table_staging(table_name)
        self.execute(query)
        
    
    
    

    