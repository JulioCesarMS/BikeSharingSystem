from src.database.tables import table_dim_station


# dimensión estación
def create_dim_station(db):
    query = table_dim_station()
    db.execute(query)