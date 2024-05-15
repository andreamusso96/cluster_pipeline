import numpy as np
from sqlalchemy import text
import pandas as pd

from src.python.utils import run_sql_script_on_db, DB, get_db_engine
from src.python.postgis_raster_io import load_raster, dump_raster
from src.python.convolution import get_2d_exponential_kernel, convolve2d
from src.python.multi_year_matching import get_cluster_year_connected_component_table
from config import config


def initialize_db():
    _clusterdb_config()
    _create_function__template_usa_raster()


def _clusterdb_config():
    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)
    with e.begin() as conn:
        conn.execute(text("ALTER DATABASE clusterdb SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';"))


@run_sql_script_on_db(db=DB.CLUSTERDB_POSTGRES)
def _create_function__template_usa_raster():
    sql_file_path = config.path.sql.ipums_tcc.create_function__template_usa_raster
    return sql_file_path, {}


def rasterize_census_places():
    for y in config.param.years:
        _rasterize_census_places(y)


@run_sql_script_on_db(db=DB.CLUSTERDB_POSTGRES)
def _rasterize_census_places(y: int):
    sql_file_path = config.path.sql.ipums_tcc.rasterize_census_places
    params = {
        'rasterized_census_places_table': config.db.table_names.rasterized_census_places.format(year=y),
        'census_place_industry_count_table': config.db.table_names.census_place_industry_count.format(year=y),
    }
    return sql_file_path, params


def create_convolved_census_place_raster():
    for y in config.param.years:
        _create_convolved_census_place_raster(y)


def _create_convolved_census_place_raster(y: int) -> None:
    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)

    # Drop table for idempotency
    with e.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {config.db.table_names.convolved_census_place_raster.format(year=y)}"))

    with e.begin() as conn:
        raster = load_raster(con=conn, raster_table=config.db.table_names.rasterized_census_places.format(year=y))
        raster_vals = raster.sel(band=1).values

        kernel = get_2d_exponential_kernel(size=config.param.convolution_kernel_size, decay_rate=config.param.convolution_kernel_decay_rate)
        convolved_raster_vals = convolve2d(image=raster_vals, kernel=kernel)

        convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
        dump_raster(con=conn, data=convolved_raster, table_name=config.db.table_names.convolved_census_place_raster.format(year=y))


def create_cluster():
    for y in config.param.years:
        _create_cluster(y)


@run_sql_script_on_db(db=DB.CLUSTERDB_POSTGRES)
def _create_cluster(y: int):
    sql_file_path = config.path.sql.ipums_tcc.create_cluster
    params = {
        'cluster_table': config.db.table_names.cluster.format(year=y),
        'cluster_industry_table': config.db.table_names.cluster_industry.format(year=y),
        'census_place_industry_count_table': config.db.table_names.census_place_industry_count.format(year=y),
        'convolved_census_place_raster_table': config.db.table_names.convolved_census_place_raster.format(year=y),
        'census_place_table': config.db.table_names.census_place,
        'industry_table': config.db.table_names.industry_code,
        'dbscan_eps': config.param.dbscan_eps,
        'dbscan_minpoints': config.param.dbscan_min_points,
        'pixel_threshold': config.param.pixel_threshold
    }
    return sql_file_path, params


def create_multiyear_tables_and_cluster_intersection_matching():
    _create_multiyear_table(base_table_name=config.db.table_names.cluster, multiyear_table_name=config.db.table_names.multiyear_cluster,
                            column_names=['cluster_id', 'population', 'geom'], create_spatial_index=True)
    _create_cluster_intersection_matching()

    _create_multiyear_table(base_table_name=config.db.table_names.cluster_industry,
                            multiyear_table_name=config.db.table_names.multiyear_cluster_industry,
                            column_names=['cluster_id', 'ind1950', 'worker_count'], create_spatial_index=False)


def _create_multiyear_table(base_table_name, multiyear_table_name, column_names, create_spatial_index: bool):
    query = ""
    for i, y in enumerate(config.param.years):
        query_year = (f"SELECT {y} as year, {', '.join(column_names)} "
                      f"FROM {base_table_name.format(year=y)} ")

        if i < len(config.param.years) - 1:
            query_year += "UNION ALL "

        query += query_year

    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)
    with e.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {multiyear_table_name}"))
        conn.execute(text(f"CREATE TABLE {multiyear_table_name} AS ({query})"))
        if create_spatial_index:
            conn.execute(text(f"CREATE INDEX ON {multiyear_table_name} USING GIST (geom)"))


@run_sql_script_on_db(db=DB.CLUSTERDB_POSTGRES)
def _create_cluster_intersection_matching():
    sql_file_path = config.path.sql.ipums_tcc.create_cluster_intersection_matching
    params = {
        'multiyear_cluster_table': config.db.table_names.multiyear_cluster,
        'cluster_intersection_matching_table': config.db.table_names.cluster_intersection_matching
    }
    return sql_file_path, params


def create_crosswalk_cluster_uid_to_cluster_id() -> None:
    e = get_db_engine(db=DB.CLUSTERDB_POSTGRES)

    with e.connect() as conn:
        matching = pd.read_sql(f"SELECT * FROM {config.db.table_names.cluster_intersection_matching}", con=conn)

    cluster_year_connected_component = get_cluster_year_connected_component_table(intersection_matching=matching)
    cluster_year_connected_component = cluster_year_connected_component.rename(columns={"component_id": "cluster_uid", "year": "year", "cluster_id": "cluster_id"})
    cluster_year_connected_component = cluster_year_connected_component[["cluster_uid", "year", "cluster_id"]].copy()

    with e.begin() as conn:
        cluster_year_connected_component.to_sql(name=config.db.table_names.crosswalk_cluster_uid_to_cluster_id, con=conn, index=False, if_exists='replace')


@run_sql_script_on_db(db=DB.CLUSTERDB_POSTGRES)
def create_time_consistent_cluster():
    sql_file_path = config.path.sql.ipums_tcc.create_time_consistent_cluster
    params = {
        'multiyear_cluster_table': config.db.table_names.multiyear_cluster,
        'multiyear_cluster_industry_table': config.db.table_names.multiyear_cluster_industry,
        'crosswalk_cluster_uid_to_cluster_id_table': config.db.table_names.crosswalk_cluster_uid_to_cluster_id,
        'time_consistent_cluster_table': config.db.table_names.time_consistent_cluster,
        'time_consistent_cluster_industry_table': config.db.table_names.time_consistent_cluster_industry,
        'industry_table': config.db.table_names.industry_code,
    }
    return sql_file_path, params