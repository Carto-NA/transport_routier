/* TRANSPORT ROUTIER V1.0 */
/* Creation de la structure des données (schéma, tables, séquences, triggers,...) */
/* transport_routier_10_structure.sql */
/* PostgreSQL/PostGIS */
/* Conseil régional Nouvelle-Aquitaine - https://cartographie.nouvelle-aquitaine.fr/ */
/* Auteur : Tony VINCENT */

------------------------------------------------------------------------ 
-- Schéma : Création du schéma
------------------------------------------------------------------------

-- Schema: gtfs_test
CREATE SCHEMA IF NOT EXISTS gtfs_test;
--SET search_path to gtfs_test, public;

COMMENT ON SCHEMA gtfs_test IS 'Schéma pour les données métiers sur le transport routier';

GRANT ALL ON SCHEMA gtfs_test TO "pre-sig-usr";
GRANT ALL ON SCHEMA gtfs_test TO "pre-sig-ro";

DROP TABLE IF EXISTS gtfs_test.agency cascade;
DROP TABLE IF EXISTS gtfs_test.stops cascade;
DROP TABLE IF EXISTS gtfs_test.routes cascade;
DROP TABLE IF EXISTS gtfs_test.calendar cascade;
DROP TABLE IF EXISTS gtfs_test.calendar_dates cascade;
DROP TABLE IF EXISTS gtfs_test.fare_attributes cascade;
DROP TABLE IF EXISTS gtfs_test.fare_rules cascade;
DROP TABLE IF EXISTS gtfs_test.shapes cascade;
DROP TABLE IF EXISTS gtfs_test.trips cascade;
DROP TABLE IF EXISTS gtfs_test.stop_times cascade;
DROP TABLE IF EXISTS gtfs_test.frequencies cascade;
DROP TABLE IF EXISTS gtfs_test.shape_geoms CASCADE;
DROP TABLE IF EXISTS gtfs_test.transfers cascade;
DROP TABLE IF EXISTS gtfs_test.timepoints cascade;
DROP TABLE IF EXISTS gtfs_test.feed_info cascade;
DROP TABLE IF EXISTS gtfs_test.route_types cascade;
DROP TABLE IF EXISTS gtfs_test.pickup_dropoff_types cascade;
DROP TABLE IF EXISTS gtfs_test.payment_methods cascade;
DROP TABLE IF EXISTS gtfs_test.location_types cascade;
DROP TABLE IF EXISTS gtfs_test.exception_types cascade;
DROP TABLE IF EXISTS gtfs_test.wheelchair_boardings cascade;
DROP TABLE IF EXISTS gtfs_test.wheelchair_accessible cascade;
DROP TABLE IF EXISTS gtfs_test.transfer_types cascade;


------------------------------------------------------------------------ 
-- Tables : Création des tables
------------------------------------------------------------------------

------------------------------------------------------------------------
-- Table: gtfs_test.feed_info
-- DROP TABLE gtfs_test.feed_info;
CREATE TABLE gtfs_test.feed_info (
  feed_index serial PRIMARY KEY, 
  feed_publisher_name text default null,
  feed_publisher_url text default null,
  feed_timezone text default null,
  feed_lang text default null,
  feed_version text default null,
  feed_start_date date default null,
  feed_end_date date default null,
  feed_id text default null,
  feed_contact_url text default null,
  feed_download_date date,
  feed_file text
);


------------------------------------------------------------------------
-- Table: gtfs_test.agency
-- DROP TABLE gtfs_test.agency;
CREATE TABLE gtfs_test.agency (
  feed_index integer REFERENCES gtfs_test.feed_info (feed_index),
  agency_id text default '',
  agency_name text default null,
  agency_url text default null,
  agency_timezone text default null,
  -- optional
  agency_lang text default null,
  agency_phone text default null,
  agency_fare_url text default null,
  agency_email text default null,
  bikes_policy_url text default null,
  CONSTRAINT agency_pkey PRIMARY KEY (feed_index, agency_id)
);


------------------------------------------------------------------------
-- Table: gtfs_test.exception_types
--related to calendar_dates(exception_type)
-- DROP TABLE gtfs_test.exception_types;
CREATE TABLE gtfs_test.exception_types (
  exception_type int PRIMARY KEY,
  description text
);


------------------------------------------------------------------------
-- Table: gtfs_test.wheelchair_accessible
--related to stops(wheelchair_accessible)
-- DROP TABLE gtfs_test.wheelchair_accessible;
CREATE TABLE gtfs_test.wheelchair_accessible (
  wheelchair_accessible int PRIMARY KEY,
  description text
);


------------------------------------------------------------------------
-- Table: gtfs_test.wheelchair_boardings
--related to stops(wheelchair_boarding)
-- DROP TABLE gtfs_test.wheelchair_boardings;
CREATE TABLE gtfs_test.wheelchair_boardings (
  wheelchair_boarding int PRIMARY KEY,
  description text
);


------------------------------------------------------------------------
-- Table: gtfs_test.pickup_dropoff_types
-- DROP TABLE gtfs_test.pickup_dropoff_types;
CREATE TABLE gtfs_test.pickup_dropoff_types (
  type_id int PRIMARY KEY,
  description text
);


------------------------------------------------------------------------
-- Table: gtfs_test.transfer_types
-- DROP TABLE gtfs_test.transfer_types;
CREATE TABLE gtfs_test.transfer_types (
  transfer_type int PRIMARY KEY,
  description text
);


------------------------------------------------------------------------
-- Table: gtfs_test.location_types
--related to stops(location_type)
-- DROP TABLE gtfs_test.location_types;
CREATE TABLE gtfs_test.location_types (
  location_type int PRIMARY KEY,
  description text
);


------------------------------------------------------------------------
-- Table: gtfs_test.timepoints
-- related to stop_times(timepoint)
-- DROP TABLE gtfs_test.timepoints;
CREATE TABLE gtfs_test.timepoints (
  timepoint int PRIMARY KEY,
  description text
);


------------------------------------------------------------------------
-- Table: gtfs_test.calendar
-- DROP TABLE gtfs_test.calendar;
CREATE TABLE gtfs_test.calendar (
  feed_index integer not null,
  service_id text,
  monday int not null,
  tuesday int not null,
  wednesday int not null,
  thursday int not null,
  friday int not null,
  saturday int not null,
  sunday int not null,
  start_date date not null,
  end_date date not null,
  CONSTRAINT calendar_pkey PRIMARY KEY (feed_index, service_id),
  CONSTRAINT calendar_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);
CREATE INDEX calendar_service_id ON gtfs_test.calendar (service_id);


------------------------------------------------------------------------
-- Table: gtfs_test.stops
-- DROP TABLE gtfs_test.stops;
CREATE TABLE gtfs_test.stops (
  feed_index int not null,
  stop_id text,
  stop_name text default null,
  stop_desc text default null,
  stop_lat double precision,
  stop_lon double precision,
  zone_id text,
  stop_url text,
  stop_code text,
  stop_street text,
  stop_city text,
  stop_region text,
  stop_postcode text,
  stop_country text,
  stop_timezone text,
  direction text,
  position text default null,
  parent_station text default null,
  wheelchair_boarding integer default null REFERENCES gtfs_test.wheelchair_boardings (wheelchair_boarding),
  wheelchair_accessible integer default null REFERENCES gtfs_test.wheelchair_accessible (wheelchair_accessible),
  -- optional
  location_type integer default null REFERENCES gtfs_test.location_types (location_type),
  vehicle_type int default null,
  platform_code text default null,
  CONSTRAINT stops_pkey PRIMARY KEY (feed_index, stop_id)
);
SELECT AddGeometryColumn('gtfs_test', 'stops', 'the_geom', 4326, 'POINT', 2);

--
GRANT ALL ON TABLE gtfs_test.stops TO "pre-sig-usr";
GRANT SELECT ON TABLE gtfs_test.stops TO "pre-sig-ro";

--
TRUNCATE TABLE gtfs_test.stops;
INSERT INTO gtfs_test.stops(feed_index,stop_id, stop_name, stop_desc, stop_lat, stop_lon, parent_station, wheelchair_boarding, location_type)
SELECT 2,stop_id, stop_name, stop_desc, stop_lat, stop_lon, parent_station, wheelchair_boarding,  location_type
FROM z_maj.gtfs_stops;


-- trigger the_geom update with lat or lon inserted
CREATE OR REPLACE FUNCTION gtfs_test.stop_geom_update() RETURNS TRIGGER AS $stop_geom$
  BEGIN
    NEW.the_geom = ST_SetSRID(ST_MakePoint(NEW.stop_lon, NEW.stop_lat), 4326);
    RETURN NEW;
  END;
$stop_geom$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS stop_geom_trigger ON gtfs_test.stops;
CREATE TRIGGER stop_geom_trigger BEFORE INSERT OR UPDATE ON gtfs_test.stops
    FOR EACH ROW EXECUTE PROCEDURE stop_geom_update();

CREATE TABLE gtfs_test.route_types (
  route_type int PRIMARY KEY,
  description text
);



CREATE TABLE gtfs_test.routes (
  feed_index int not null,
  route_id text,
  agency_id text,
  route_short_name text default '',
  route_long_name text default '',
  route_desc text default '',
  route_type int REFERENCES gtfs_test.route_types(route_type),
  route_url text,
  route_color text,
  route_text_color text,
  -- unofficial
  route_sort_order integer default null,
  CONSTRAINT routes_pkey PRIMARY KEY (feed_index, route_id),
  -- CONSTRAINT routes_fkey FOREIGN KEY (feed_index, agency_id)
  --   REFERENCES agency (feed_index, agency_id),
  CONSTRAINT routes_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);

--
GRANT ALL ON TABLE gtfs_test.routes TO "pre-sig-usr";
GRANT SELECT ON TABLE gtfs_test.routes TO "pre-sig-ro";

-- 
TRUNCATE TABLE gtfs_test.routes;
INSERT INTO gtfs_test.routes
(feed_index, route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color)
SELECT 2, route_id, agency_id, route_short_name::text, route_long_name::text, route_desc::text, route_type, route_url, route_color, route_text_color
FROM z_maj.gtfs_routes;



CREATE TABLE gtfs_test.calendar_dates (
  feed_index int not null,
  service_id text,
  date date not null,
  exception_type int REFERENCES gtfs_test.exception_types(exception_type) --,
  -- CONSTRAINT calendar_fkey FOREIGN KEY (feed_index, service_id)
    -- REFERENCES calendar (feed_index, service_id)
);

CREATE INDEX calendar_dates_dateidx ON gtfs_test.calendar_dates (date);



CREATE TABLE gtfs_test.payment_methods (
  payment_method int PRIMARY KEY,
  description text
);

CREATE TABLE gtfs_test.fare_attributes (
  feed_index int not null,
  fare_id text not null,
  price double precision not null,
  currency_type text not null,
  payment_method int REFERENCES gtfs_test.payment_methods,
  transfers int,
  transfer_duration int,
  -- unofficial features
  agency_id text default null,
  CONSTRAINT fare_attributes_pkey PRIMARY KEY (feed_index, fare_id),
  -- CONSTRAINT fare_attributes_fkey FOREIGN KEY (feed_index, agency_id)
  -- REFERENCES agency (feed_index, agency_id),
  CONSTRAINT fare_attributes_fare_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);

CREATE TABLE gtfs_test.fare_rules (
  feed_index int not null,
  fare_id text,
  route_id text,
  origin_id text,
  destination_id text,
  contains_id text,
  -- unofficial features
  service_id text default null,
  -- CONSTRAINT fare_rules_service_fkey FOREIGN KEY (feed_index, service_id)
  -- REFERENCES calendar (feed_index, service_id),
  -- CONSTRAINT fare_rules_fare_id_fkey FOREIGN KEY (feed_index, fare_id)
  -- REFERENCES fare_attributes (feed_index, fare_id),
  -- CONSTRAINT fare_rules_route_id_fkey FOREIGN KEY (feed_index, route_id)
  -- REFERENCES routes (feed_index, route_id),
  CONSTRAINT fare_rules_service_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);

CREATE TABLE gtfs_test.shapes (
  feed_index int not null,
  shape_id text not null,
  shape_pt_lat double precision not null,
  shape_pt_lon double precision not null,
  shape_pt_sequence int not null,
  -- optional
  shape_dist_traveled double precision default null
);

CREATE INDEX shapes_shape_key ON gtfs_test.shapes (shape_id);

CREATE OR REPLACE FUNCTION gtfs_test.shape_update()
  RETURNS TRIGGER AS $$
  BEGIN
    INSERT INTO gtfs_test.shape_geoms
      (feed_index, shape_id, length, the_geom)
    SELECT
      feed_index,
      shape_id,
      ST_Length(ST_MakeLine(array_agg(geom ORDER BY shape_pt_sequence))::geography) as length,
      ST_SetSRID(ST_MakeLine(array_agg(geom ORDER BY shape_pt_sequence)), 4326) AS the_geom
    FROM (
      SELECT
        feed_index,
        shape_id,
        shape_pt_sequence,
        ST_MakePoint(shape_pt_lon, shape_pt_lat) AS geom
      FROM gtfs_test.shapes s
        LEFT JOIN gtfs_test.shape_geoms sg USING (feed_index, shape_id)
      WHERE the_geom IS NULL
    ) a GROUP BY feed_index, shape_id;
  RETURN NULL;
  END;
  $$ LANGUAGE plpgsql
  SET search_path = gtfs_test, public;

DROP TRIGGER IF EXISTS shape_geom_trigger ON gtfs_test.shapes;
CREATE TRIGGER shape_geom_trigger AFTER INSERT ON gtfs_test.shapes
  FOR EACH STATEMENT EXECUTE PROCEDURE shape_update();
  
-- Tony
INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, 'VIE_L107_SHP', ST_Y(geom), ST_X(geom), cast(shp_pt_seq as integer) FROM (select shp_pt_seq, (ST_Dump(geom)).geom from z_maj.gtfs_traces_pt) as foo;
 ST_Y((geom) as latitude, ST_X((geom) as longitude, shp_pt_seq FROM z_maj.gtfs_traces_pt order by shp_pt_seq asc;

INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, route_id, ST_Y(geom), ST_X(geom), cast(shp_pt_seq as integer) FROM (select route_id, shp_pt_seq, (ST_Dump(geom)).geom from z_maj.gtfs_traces_pt_1 where route_id = 'ANG:Line:13') as foo;

INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, route_id, ST_Y(geom), ST_X(geom), cast(shp_pt_seq as integer) FROM (select route_id, shp_pt_seq, (ST_Dump(geom)).geom from z_maj.gtfs_traces_pt_2) as foo; 


INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, field_18, cast(resultat_y as float8), cast(resultat_x as float8), field_16 FROM z_maj.gtfs_shaps_lim28 where field_16 is not null ; 
INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, field_18, cast(resultat_y as float8), cast(resultat_x as float8), cast(field_17 as integer) FROM z_maj.gtfs_shaps_lim28 where field_17 is not null;

-- Tony, le 06/01/2020
INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, route_id, cast(resultat_y as float8), cast(resultat_x as float8), cast(resultat_seq1 as integer) FROM z_maj."hvi_LineR26_20190901" where resultat_seq1 is not null ; 

INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, route_id, cast(resultat_y as float8), cast(resultat_x as float8), cast(resultat_seq2 as integer) FROM z_maj."hvi_LineR26_20190901" where resultat_seq2 is not null;


-- Tony, le 30/01/2020
INSERT INTO gtfs_test.shapes (feed_index, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) 
SELECT 2, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence FROM z_maj.gtfs_79_shapes; 

------------------------------------------------------------------------

SELECT replace(route_id,':Line:','') FROM gtfs_test.routes;

------------------------------------------------------------------------


-- Create new table to store the shape geometries
CREATE TABLE gtfs_test.shape_geoms (
  feed_index int not null,
  shape_id text not null,
  length numeric(12, 2) not null,
  CONSTRAINT shape_geom_pkey PRIMARY KEY (feed_index, shape_id)
);
-- Add the_geom column to the shape_geoms table - a 2D linestring geometry
SELECT AddGeometryColumn('gtfs_test', 'shape_geoms', 'the_geom', 4326, 'LINESTRING', 2);

--
GRANT ALL ON TABLE gtfs_test.shape_geoms TO "pre-sig-usr";
GRANT SELECT ON TABLE gtfs_test.shape_geoms TO "pre-sig-ro";


CREATE TABLE gtfs_test.trips (
  feed_index int not null,
  route_id text not null,
  service_id text not null,
  trip_id text not null,
  trip_headsign text,
  direction_id int,
  block_id text,
  shape_id text,
  trip_short_name text,
  wheelchair_accessible int REFERENCES gtfs_test.wheelchair_accessible(wheelchair_accessible),

  -- unofficial features
  direction text default null,
  schd_trip_id text default null,
  trip_type text default null,
  exceptional int default null,
  bikes_allowed int default null,
  CONSTRAINT trips_pkey PRIMARY KEY (feed_index, trip_id),
  -- CONSTRAINT trips_route_id_fkey FOREIGN KEY (feed_index, route_id)
  -- REFERENCES routes (feed_index, route_id),
  -- CONSTRAINT trips_calendar_fkey FOREIGN KEY (feed_index, service_id)
  -- REFERENCES calendar (feed_index, service_id),
  CONSTRAINT trips_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);

CREATE INDEX trips_trip_id ON gtfs_test.trips (trip_id);
CREATE INDEX trips_service_id ON gtfs_test.trips (feed_index, service_id);

-- Tony
INSERT INTO gtfs_test.trips
(feed_index, route_id, service_id, trip_id, trip_headsign, direction_id, block_id)
SELECT 2, route_id, service_id, trip_id, trip_headsign, direction_id, block_id
FROM z_maj.gtfs_trips;

--30/01/2020
TRUNCATE TABLE gtfs_test.trips;
INSERT INTO gtfs_test.trips
(feed_index, route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, trip_short_name, wheelchair_accessible)
SELECT 2, route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, trip_short_name, wheelchair_accessible 
FROM z_maj.gtfs_79_trips;
------------------------------------------------------------------------



CREATE TABLE gtfs_test.stop_times (
  feed_index int not null,
  trip_id text not null,
  -- Check that casting to time interval works.
  arrival_time interval CHECK (arrival_time::interval = arrival_time::interval),
  departure_time interval CHECK (departure_time::interval = departure_time::interval),
  stop_id text,
  stop_sequence int not null,
  stop_headsign text,
  pickup_type int REFERENCES gtfs_test.pickup_dropoff_types(type_id),
  drop_off_type int REFERENCES gtfs_test.pickup_dropoff_types(type_id),
  shape_dist_traveled numeric(10, 2),
  timepoint int REFERENCES gtfs_test.timepoints (timepoint),

  -- unofficial features
  -- the following are not in the spec
  continuous_drop_off int default null,
  continuous_pickup  int default null,
  arrival_time_seconds int default null,
  departure_time_seconds int default null,
  CONSTRAINT stop_times_pkey PRIMARY KEY (feed_index, trip_id, stop_sequence),
  -- CONSTRAINT stop_times_trips_fkey FOREIGN KEY (feed_index, trip_id)
  -- REFERENCES trips (feed_index, trip_id),
  -- CONSTRAINT stop_times_stops_fkey FOREIGN KEY (feed_index, stop_id)
  -- REFERENCES stops (feed_index, stop_id),
  CONSTRAINT stop_times_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);
CREATE INDEX stop_times_key ON gtfs_test.stop_times (feed_index, trip_id, stop_id);
CREATE INDEX arr_time_index ON gtfs_test.stop_times (arrival_time_seconds);
CREATE INDEX dep_time_index ON gtfs_test.stop_times (departure_time_seconds);

-- Tony
INSERT INTO gtfs_test.stop_times
(feed_index, trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, timepoint)
SELECT 2, trip_id, arrival_time::interval, departure_time::interval, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, cast(timepoint AS integer)
FROM z_maj.gtfs_stop_times;


-- "Safely" locate a point on a (possibly complicated) line by using minimum and maximum distances.
-- Unlike st_LineLocatePoint, this accepts and returns absolute distances, not fractions
CREATE OR REPLACE FUNCTION gtfs_test.safe_locate
  (route geometry, point geometry, start numeric, finish numeric, length numeric)
  RETURNS numeric AS $$
    -- Multiply the fractional distance also the substring by the substring,
    -- then add the start distance
    SELECT LEAST(length, GREATEST(0, start) + ST_LineLocatePoint(
      ST_LineSubstring(route, GREATEST(0, start / length), LEAST(1, finish / length)),
      point
    )::numeric * (
      -- The absolute distance between start and finish
      LEAST(length, finish) - GREATEST(0, start)
    ));
  $$ LANGUAGE SQL;

-- Fill in the shape_dist_traveled field using stop and shape geometries. 
CREATE OR REPLACE FUNCTION gtfs_test.dist_insert()
  RETURNS TRIGGER AS $$
  BEGIN
  NEW.shape_dist_traveled := (
    SELECT
      ST_LineLocatePoint(route.the_geom, stop.the_geom) * route.length
    FROM gtfs_test.stops as stop
      LEFT JOIN gtfs_test.trips ON (stop.feed_index=trips.feed_index AND trip_id=NEW.trip_id)
      LEFT JOIN gtfs_test.shape_geoms AS route ON (route.feed_index = stop.feed_index and trips.shape_id = route.shape_id)
      WHERE stop_id = NEW.stop_id
        AND stop.feed_index = COALESCE(NEW.feed_index::integer, (
          SELECT column_default::integer
          FROM information_schema.columns
          WHERE (table_schema, table_name, column_name) = (TG_TABLE_SCHEMA, 'stop_times', 'feed_index')
        ))
  )::NUMERIC;
  RETURN NEW;
  END;
  $$
  LANGUAGE plpgsql
  SET search_path = gtfs_test, public;

DROP TRIGGER IF EXISTS stop_times_dist_row_trigger ON gtfs_test.stop_times;
CREATE TRIGGER stop_times_dist_row_trigger BEFORE INSERT ON gtfs_test.stop_times
  FOR EACH ROW
  WHEN (NEW.shape_dist_traveled IS NULL)
  EXECUTE PROCEDURE dist_insert();

-- Correct out-of-order shape_dist_traveled fields.
CREATE OR REPLACE FUNCTION gtfs_test.dist_update()
  RETURNS TRIGGER AS $$
  BEGIN
  WITH f AS (SELECT MAX(feed_index) AS feed_index FROM gtfs_test.feed_info)
  UPDATE gtfs_test.stop_times s
    SET shape_dist_traveled = safe_locate(r.the_geom, p.the_geom, lag::numeric, coalesce(lead, length)::numeric, length::numeric)
  FROM
    (
      SELECT
        feed_index,
        trip_id,
        stop_id,
        coalesce(lag(shape_dist_traveled) over (trip), 0) AS lag,
        shape_dist_traveled AS dist,
        lead(shape_dist_traveled) over (trip) AS lead
      FROM gtfs_test.stop_times
        INNER JOIN f USING (feed_index)
      WINDOW trip AS (PARTITION BY feed_index, trip_id ORDER BY stop_sequence)
    ) AS d
    LEFT JOIN gtfs_test.stops AS p USING (feed_index, stop_id)
    LEFT JOIN gtfs_test.trips USING (feed_index, trip_id)
    LEFT JOIN gtfs_test.shape_geoms r USING (feed_index, shape_id)
  WHERE (s.feed_index, s.trip_id, s.stop_id) = (d.feed_index, d.trip_id, d.stop_id)
    AND COALESCE(lead, length) > lag
    AND (dist > COALESCE(lead, length) OR dist < lag);
  RETURN NULL;
  END;
  $$ LANGUAGE plpgsql
  SET search_path = gtfs_test, public;

DROP TRIGGER IF EXISTS stop_times_dist_stmt_trigger ON gtfs_test.stop_times;
CREATE TRIGGER stop_times_dist_stmt_trigger AFTER INSERT ON gtfs_test.stop_times
  FOR EACH STATEMENT EXECUTE PROCEDURE dist_update();

CREATE TABLE gtfs_test.frequencies (
  feed_index int not null,
  trip_id text,
  start_time text not null CHECK (start_time::interval = start_time::interval),
  end_time text not null CHECK (end_time::interval = end_time::interval),
  headway_secs int not null,
  exact_times int,
  start_time_seconds int,
  end_time_seconds int,
  CONSTRAINT frequencies_pkey PRIMARY KEY (feed_index, trip_id, start_time),
  -- CONSTRAINT frequencies_trip_fkey FOREIGN KEY (feed_index, trip_id)
  --  REFERENCES trips (feed_index, trip_id),
  CONSTRAINT frequencies_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);

CREATE TABLE gtfs_test.transfers (
  feed_index int not null,
  from_stop_id text,
  to_stop_id text,
  transfer_type int REFERENCES gtfs_test.transfer_types(transfer_type),
  min_transfer_time int,
  -- Unofficial fields
  from_route_id text default null,
  to_route_id text default null,
  service_id text default null,
  -- CONSTRAINT transfers_from_stop_fkey FOREIGN KEY (feed_index, from_stop_id)
  --  REFERENCES stops (feed_index, stop_id),
  --CONSTRAINT transfers_to_stop_fkey FOREIGN KEY (feed_index, to_stop_id)
  --  REFERENCES stops (feed_index, stop_id),
  --CONSTRAINT transfers_from_route_fkey FOREIGN KEY (feed_index, from_route_id)
  --  REFERENCES routes (feed_index, route_id),
  --CONSTRAINT transfers_to_route_fkey FOREIGN KEY (feed_index, to_route_id)
  --  REFERENCES routes (feed_index, route_id),
  --CONSTRAINT transfers_service_fkey FOREIGN KEY (feed_index, service_id)
  --  REFERENCES calendar (feed_index, service_id),
  CONSTRAINT transfers_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs_test.feed_info (feed_index) ON DELETE CASCADE
);

insert into gtfs_test.exception_types (exception_type, description) values 
  (1, 'service has been added'),
  (2, 'service has been removed');

insert into gtfs_test.transfer_types (transfer_type, description) VALUES
  (0,'Preferred transfer point'),
  (1,'Designated transfer point'),
  (2,'Transfer possible with min_transfer_time window'),
  (3,'Transfers forbidden');

insert into gtfs_test.location_types(location_type, description) values 
  (0,'stop'),
  (1,'station'),
  (2,'station entrance');

insert into gtfs_test.wheelchair_boardings(wheelchair_boarding, description) values
   (0, 'No accessibility information available for the stop'),
   (1, 'At least some vehicles at this stop can be boarded by a rider in a wheelchair'),
   (2, 'Wheelchair boarding is not possible at this stop');

insert into gtfs_test.wheelchair_accessible(wheelchair_accessible, description) values
  (0, 'No accessibility information available for this trip'),
  (1, 'The vehicle being used on this particular trip can accommodate at least one rider in a wheelchair'),
  (2, 'No riders in wheelchairs can be accommodated on this trip');

insert into gtfs_test.pickup_dropoff_types (type_id, description) values
  (0,'Regularly Scheduled'),
  (1,'Not available'),
  (2,'Phone arrangement only'),
  (3,'Driver arrangement only');

insert into gtfs_test.payment_methods (payment_method, description) values
  (0,'On Board'),
  (1,'Prepay');

insert into gtfs_test.timepoints (timepoint, description) values
  (0, 'Times are considered approximate'),
  (1, 'Times are considered exact');

COMMIT;


------------------------------------------------------------------------

-- Avoir les arrêts par lignes
drop materialized view gtfs_test.trips_width_stops;
CREATE MATERIALIZED VIEW gtfs_test.trips_width_stops AS
/*select distinct t1.route_id, t1.trajet as trip_id, t1.direction_id, t2.stop_id, b.stop_name, b.stop_desc, b.the_geom
from (select distinct route_id, split_part(trip_id, '_', 1) as trajet, direction_id  from gtfs_test.trips) t1
inner join (select stop_id, split_part(trip_id, '-', 1) as trip_id from gtfs_test.stop_times) t2 on t1.trajet=t2.trip_id
inner join gtfs_test.stops b on t2.stop_id = b.stop_id;*/
select distinct t1.route_id, t1.trajet as trip_id, t1.direction_id, t2.stop_id, b.stop_name, b.stop_desc, b.the_geom
from (select distinct route_id, split_part(trip_id, '_', 1) as trajet, direction_id  from gtfs_test.trips) t1
inner join (select stop_id, split_part(split_part(trip_id, '-', 1), '_', 1) as trip_id from gtfs_test.stop_times) t2 on t1.trajet=t2.trip_id
inner join gtfs_test.stops b on t2.stop_id = b.stop_id;


CREATE UNIQUE INDEX trips_width_stops_uniq
  ON gtfs_test.trips_width_stops (route_id, trip_id,stop_id, direction_id);
  
--
GRANT ALL ON TABLE gtfs_test.trips_width_stops TO "pre-sig-usr";
GRANT SELECT ON TABLE gtfs_test.trips_width_stops TO "pre-sig-ro";



-- Avoir les itinéraires
DROP MATERIALIZED VIEW gtfs_test.route_width_trips;
CREATE MATERIALIZED VIEW gtfs_test.route_width_trips AS
SELECT t1.*, t2.service_id, t2.shape_id, t2.direction_id,t3.the_geom AS geom FROM gtfs_test.routes t1
INNER JOIN (SELECT * FROM gtfs_test.trips) t2 ON t1.route_id = t2.route_id
INNER JOIN (SELECT * FROM gtfs_test.shape_geoms) t3 ON t2.shape_id = t3.shape_id;


CREATE UNIQUE INDEX route_width_trips_uniq
  ON gtfs_test.route_width_trips (route_id, service_id,shape_id);
  
--
GRANT ALL ON TABLE gtfs_test.route_width_trips TO "pre-sig-usr";
GRANT SELECT ON TABLE gtfs_test.route_width_trips TO "pre-sig-ro";



CREATE TABLE gtfs_test.lignes86
(
    id serial NOT NULL,
    route_id character varying(255),
	direction character varying(1),
    geom geometry(MultiLineString,2154),
    CONSTRAINT lignes86_pkey PRIMARY KEY (id)
);
    

GRANT ALL ON TABLE gtfs_test.lignes86 TO "pre-sig-usr";
GRANT ALL ON TABLE gtfs_test.lignes86 TO "pre-sig-ro";
GRANT ALL ON SEQUENCE gtfs_test.lignes86_id_seq TO "pre-sig-ro";
