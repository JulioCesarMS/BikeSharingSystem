

CREATE TABLE viajes (
  Genero_Usuario char(1) DEFAULT NULL,
  Edad_Usuario int DEFAULT NULL,
  Bici bigint DEFAULT NULL,
  Ciclo_Estacion_Retiro varchar(20) DEFAULT NULL,
  Fecha_Retiro date DEFAULT NULL,
  Hora_Retiro time DEFAULT NULL,
  Ciclo_Estacion_Arribo varchar(20) DEFAULT NULL,
  Fecha_Arribo date DEFAULT NULL,
  Hora_Arribo time DEFAULT NULL,
  Nombre_Archivo VARCHAR(255) DEFAULT NULL
);