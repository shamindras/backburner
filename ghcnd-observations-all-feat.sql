/* Start query to create ghcnd_observations_prcp ---------------------------------*/
DROP TABLE IF EXISTS ghcnd_observations_prcp;

CREATE TABLE ghcnd_observations_prcp AS
(SELECT ghobs.record_dt,
        ghobs."PRCP",
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM ghcnd_observations ghobs
 WHERE ghobs."PRCP" IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX ghcnd_observations_prcp_location ON ghcnd_observations_prcp USING GIST (location);
CREATE INDEX ghcnd_observations_prcp_dt ON ghcnd_observations_prcp (record_dt);

/* End query to create ghcnd_observations_prcp ----------------------------------*/

/* Start query to create ghcnd_observations_snow ---------------------------------*/
DROP TABLE IF EXISTS ghcnd_observations_snow;

CREATE TABLE ghcnd_observations_snow AS
(SELECT ghobs.record_dt,
        ghobs."SNOW",
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM ghcnd_observations ghobs
 WHERE ghobs."SNOW" IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX ghcnd_observations_snow_location ON ghcnd_observations_snow USING GIST (location);
CREATE INDEX ghcnd_observations_snow_dt ON ghcnd_observations_snow (record_dt);

/* End query to create ghcnd_observations_snow ----------------------------------*/

/* Start query to create ghcnd_observations_snwd ---------------------------------*/
DROP TABLE IF EXISTS ghcnd_observations_snwd;

CREATE TABLE ghcnd_observations_snwd AS
(SELECT ghobs.record_dt,
        ghobs."SNWD",
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM ghcnd_observations ghobs
 WHERE ghobs."SNWD" IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX ghcnd_observations_snwd_location ON ghcnd_observations_snwd USING GIST (location);
CREATE INDEX ghcnd_observations_snwd_dt ON ghcnd_observations_snwd (record_dt);

/* End query to create ghcnd_observations_snwd ----------------------------------*/

/* Start query to create ghcnd_observations_tmax ---------------------------------*/
DROP TABLE IF EXISTS ghcnd_observations_tmax;

CREATE TABLE ghcnd_observations_tmax AS
(SELECT ghobs.record_dt,
        ghobs."TMAX",
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM ghcnd_observations ghobs
 WHERE ghobs."TMAX" IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX ghcnd_observations_tmax_location ON ghcnd_observations_tmax USING GIST (location);
CREATE INDEX ghcnd_observations_tmax_dt ON ghcnd_observations_tmax (record_dt);

/* End query to create ghcnd_observations_tmax ----------------------------------*/

/* Start query to create ghcnd_observations_tmin ---------------------------------*/
DROP TABLE IF EXISTS ghcnd_observations_tmin;

CREATE TABLE ghcnd_observations_tmin AS
(SELECT ghobs.record_dt,
        ghobs."TMIN",
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM ghcnd_observations ghobs
 WHERE ghobs."TMIN" IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX ghcnd_observations_tmin_location ON ghcnd_observations_tmin USING GIST (location);
CREATE INDEX ghcnd_observations_tmin_dt ON ghcnd_observations_tmin (record_dt);

/* End query to create ghcnd_observations_tmin ----------------------------------*/

/* Start query to create ghcnd_observations_tobs ---------------------------------*/
DROP TABLE IF EXISTS ghcnd_observations_tobs;

CREATE TABLE ghcnd_observations_tobs AS
(SELECT ghobs.record_dt,
        ghobs."TOBS",
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM ghcnd_observations ghobs
 WHERE ghobs."TOBS" IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX ghcnd_observations_tobs_location ON ghcnd_observations_tobs USING GIST (location);
CREATE INDEX ghcnd_observations_tobs_dt ON ghcnd_observations_tobs (record_dt);

/* End query to create ghcnd_observations_tobs ----------------------------------*/

/* Start query to create ghcnd_observations_tavg ---------------------------------*/
DROP TABLE IF EXISTS ghcnd_observations_tavg;

CREATE TABLE ghcnd_observations_tavg AS
(SELECT ghobs.record_dt,
        ghobs."TAVG",
        ST_SetSRID(ghobs.location, 4326) AS location
 FROM ghcnd_observations ghobs
 WHERE ghobs."TAVG" IS NOT NULL);

/* Index this table on location and record_dt */
CREATE INDEX ghcnd_observations_tavg_location ON ghcnd_observations_tavg USING GIST (location);
CREATE INDEX ghcnd_observations_tavg_dt ON ghcnd_observations_tavg (record_dt);

/* End query to create ghcnd_observations_tavg ----------------------------------*/
