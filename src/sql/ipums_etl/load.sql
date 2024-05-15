DROP TABLE IF EXISTS "{{ params.census_place_table_name }}";
DROP TABLE IF EXISTS "{{ params.industry_code_table_name }}";

-- Create table for census place data
CREATE TABLE "{{ params.census_place_table_name }}" (
    lat FLOAT,
    lon FLOAT,
    consistent_place_3 INTEGER,
    consistent_place_name_3 VARCHAR(50),
    consistent_place_5 INTEGER,
    consistent_place_name_5 VARCHAR(50),
    consistent_place_10 INTEGER,
    consistent_place_name_10 VARCHAR(50),
    consistent_place_50 INTEGER,
    consistent_place_name_50 VARCHAR(50),
    consistent_place_100 INTEGER,
    consistent_place_name_100 VARCHAR(50),
    consistent_place_200 INTEGER,
    consistent_place_name_200 VARCHAR(50),
    consistent_place_300 INTEGER,
    consistent_place_name_300 VARCHAR(50),
    consistent_place_500 INTEGER,
    consistent_place_name_500 VARCHAR(50),
    fracpop FLOAT,
    potential_match VARCHAR(50),
    id INTEGER
);

COPY "{{ params.census_place_table_name }}" FROM '{{ params.census_place_file_name }}' DELIMITER ',' CSV HEADER;

-- Refactor table to include geometry columns and drop needless columns
CREATE TABLE "{{ params.census_place_table_name }}_new"
    (id INTEGER PRIMARY KEY,
     potential_match VARCHAR(50),
     geom GEOGRAPHY
    );

INSERT INTO "{{ params.census_place_table_name }}_new"
SELECT id, potential_match, ST_MakePoint(lon, lat)::GEOGRAPHY AS geom
FROM "{{ params.census_place_table_name }}";

DROP TABLE "{{ params.census_place_table_name }}";
ALTER TABLE "{{ params.census_place_table_name }}_new" RENAME TO "{{ params.census_place_table_name }}";

CREATE INDEX ON "{{ params.census_place_table_name }}" USING GIST (geom);

-- Create table for industry codes
CREATE TABLE "{{ params.industry_code_table_name }}" (
    code INTEGER,
    description VARCHAR(70),
    refined_categories VARCHAR(70),
    broad_categories VARCHAR(70),
    agri_non_agri VARCHAR(70),
    detailed VARCHAR(70),
    no_agriculture VARCHAR(70),
    all_group_by VARCHAR (70)
);

COPY "{{ params.industry_code_table_name }}" FROM '{{ params.industry_code_file_name }}' DELIMITER ',' CSV HEADER;

ALTER TABLE "{{ params.industry_code_table_name }}" ADD PRIMARY KEY (code);