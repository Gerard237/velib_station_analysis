CREATE OR REPLACE VIEW kpi_taux_occupation_station AS
SELECT
    s.nom_station,
    AVG(f.nombre_velos_disponibles::decimal / NULLIF(f.capacite_station,0)) * 100 AS taux_occupation_moyen
FROM fact_disponibilite_velib f
JOIN dim_station s ON f.station_id = s.identifiant_station
GROUP BY s.nom_station;


CREATE OR REPLACE VIEW kpi_dispo_type_velo_heure AS
SELECT
    t.heure,
    AVG(f.velos_mecaniques_disponibles) AS avg_velos_mecaniques,
    AVG(f.velos_electriques_disponibles) AS avg_velos_electriques
FROM fact_disponibilite_velib f
JOIN dim_temps t ON f.temps_id = t.temps_id
GROUP BY t.heure
ORDER BY t.heure;


CREATE OR REPLACE VIEW kpi_stations_vides AS
SELECT
    s.nom_station,
    COUNT(*) AS nb_mesures,
    SUM(CASE WHEN f.nombre_velos_disponibles = 0 THEN 1 ELSE 0 END) AS nb_sans_velos,
    (SUM(CASE WHEN f.nombre_velos_disponibles = 0 THEN 1 ELSE 0 END)::decimal / COUNT(*)) * 100 AS pct_temps_sans_velos
FROM fact_disponibilite_velib f
JOIN dim_station s ON f.station_id = s.identifiant_station
GROUP BY s.nom_station
ORDER BY pct_temps_sans_velos DESC
LIMIT 10;

CREATE OR REPLACE VIEW kpi_stations_saturees AS
SELECT
    s.nom_station,
    COUNT(*) AS nb_mesures,
    SUM(CASE WHEN f.nombre_bornettes_libres = 0 THEN 1 ELSE 0 END) AS nb_saturees,
    (SUM(CASE WHEN f.nombre_bornettes_libres = 0 THEN 1 ELSE 0 END)::decimal / COUNT(*)) * 100 AS pct_temps_saturee
FROM fact_disponibilite_velib f
JOIN dim_station s ON f.station_id = s.identifiant_station
WHERE pct_temps_saturee <> NULL
GROUP BY s.nom_station
ORDER BY pct_temps_saturee DESC
LIMIT 10;

CREATE OR REPLACE VIEW kpi_evolution_jour_station AS
SELECT
    s.nom_station,
    t.heure,
    AVG(f.nombre_velos_disponibles) AS velos_moyens
FROM fact_disponibilite_velib f
JOIN dim_temps t 
    ON f.temps_id = t.temps_id
JOIN dim_station s 
    ON f.station_id = s.identifiant_station 
GROUP BY s.nom_station, t.heure
ORDER BY s.nom_station, t.heure;
