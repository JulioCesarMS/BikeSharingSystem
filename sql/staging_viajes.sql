

CREATE TABLE IF NOT EXISTS staging_viajes (
    Genero_Usuario char(1) DEFAULT NULL,
    Edad_Usuario int DEFAULT NULL,
    Bici bigint DEFAULT NULL,
    Ciclo_Estacion_Retiro varchar(20) DEFAULT NULL,
    Fecha_Retiro date DEFAULT NULL,
    Hora_Retiro time DEFAULT NULL,
    Ciclo_Estacion_Arribo varchar(20) DEFAULT NULL,
    Fecha_Arribo date DEFAULT NULL,
    Hora_Arribo time DEFAULT NULL,
    Nombre_Archivo VARCHAR(255) DEFAULT NULL,
    INDEX idx_nombre_archivo (Nombre_Archivo),
    
    UNIQUE KEY uk_viaje (
        Genero_Usuario,
        Edad_Usuario,
        Bici,
        Ciclo_Estacion_Retiro,
        Fecha_Retiro,
        Hora_Retiro,
        Ciclo_Estacion_Arribo,
        Fecha_Arribo,
        Hora_Arribo
    )
);