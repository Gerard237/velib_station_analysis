-- DIM STATION

DROP TABLE IF EXISTS dim_station CASCADE;
CREATE TABLE dim_station (
    identifiant_station BIGINT UNIQUE NOT NULL,
    nom_station VARCHAR(255),
    capacite INT,
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7)
);

INSERT INTO dim_station (
    identifiant_station, nom_station, capacite,
    latitude, longitude
)
SELECT DISTINCT
    s.station_id,
    s.name,
    s.capacity,
    s.lat AS latitude,
    s.lon AS longitude
FROM velib_stations s;


-- DIM TEMPS

DROP TABLE IF EXISTS dim_temps CASCADE;
CREATE TABLE dim_temps (
    temps_id SERIAL PRIMARY KEY,
    horodatage TIMESTAMP UNIQUE,
    date DATE,
    annee INT,
    mois INT,
    jour INT,
    jour_semaine VARCHAR(20),
    heure INT,
    minute INT,
    weekend BOOLEAN
);

INSERT INTO dim_temps (horodatage, date, annee, mois, jour, jour_semaine, heure, minute, weekend)
SELECT DISTINCT
    d.last_reported::timestamp,
    (d.last_reported::timestamp)::date,
    EXTRACT(YEAR FROM d.last_reported::timestamp)::INT,
    EXTRACT(MONTH FROM d.last_reported::timestamp)::INT,
    EXTRACT(DAY FROM d.last_reported::timestamp)::INT,
    TO_CHAR(d.last_reported::timestamp, 'Day'),
    EXTRACT(HOUR FROM d.last_reported::timestamp)::INT,
    EXTRACT(MINUTE FROM d.last_reported::timestamp)::INT,
    CASE WHEN EXTRACT(DOW FROM d.last_reported::timestamp) IN (0,6) THEN TRUE ELSE FALSE END
FROM station_status d;


-- TABLE FAITS = chaque ligne represente le status de chaque station à l'instant t

DROP TABLE IF EXISTS fact_disponibilite_velib CASCADE;
CREATE TABLE fact_disponibilite_velib (
    fact_id BIGSERIAL PRIMARY KEY,
    station_id BIGINT REFERENCES dim_station(identifiant_station),
    temps_id INT REFERENCES dim_temps(temps_id),
    nombre_bornettes_libres INT,
    nombre_velos_disponibles INT,
    velos_mecaniques_disponibles INT,
    velos_electriques_disponibles INT,
    capacite_station INT
);

INSERT INTO fact_disponibilite_velib (
    station_id, temps_id,
    nombre_bornettes_libres, nombre_velos_disponibles,
    velos_mecaniques_disponibles, velos_electriques_disponibles,
    capacite_station
)
SELECT
    s.identifiant_station,
    t.temps_id,
    d.num_docks_available,
    d.num_bikes_available,
    d.num_mechanical,
    d.num_ebike,
    s.capacite
FROM station_status d
JOIN dim_station s
    ON d.station_id = s.identifiant_station
JOIN dim_temps t
    ON d.last_reported::timestamp = t.horodatage;
