

CREATE TABLE IF NOT EXISTS dim_station (
    station_id INT NOT NULL,
    cve_station VARCHAR(20),
    name VARCHAR(255),
    lat DECIMAL(10,7),
    lon DECIMAL(10,7),
    capacity INT,
    PRIMARY KEY (station_id)
);