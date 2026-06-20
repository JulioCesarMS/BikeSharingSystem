Module src.database.mysql_client
================================

Classes
-------

`MySQLDatabase(database=None)`
:   

    ### Methods

    `close(self)`
    :   Cierra la conexión.

    `connect(self)`
    :

    `execute_query(self, query, values=None)`
    :   Ejecuta una consulta SQL y retorna un DataFrame.

    `insertar_to_db(self, df, tabla='viajes', batch_size=5000)`
    :   Inserta un DataFrame en una tabla MySQL por lotes.
        
        Parámetros:
            df : pd.DataFrame
                DataFrame a insertar.
            tabla : str
                Nombre de la tabla destino.
            batch_size : int
                Tamaño del lote para inserciones masivas.