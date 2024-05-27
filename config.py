import os

_data_folder = "/Users/andrea/Desktop/PhD/Data/API/clusterdb/data"
_docker_data_folder = "/data"
_project_path = os.path.dirname(os.path.abspath(__file__))


class PathManager:
    class Data:
        def __init__(self, data_folder: str, docker_data_folder: str):
            self.data_folder = data_folder
            self.docker_data_folder = docker_data_folder

            self.dem = f"{self.data_folder}/ipums/census/usa_{{year}}.csv"
            self.geo = f"{self.data_folder}/ipums/geo/histid_place_crosswalk_{{year}}.csv"

            self.census_place = f"{self.docker_data_folder}/ipums/geo/place_component_crosswalk.csv"
            self.industry_code = f"{self.docker_data_folder}/ipums/census/industry1950_codes_and_desc.csv"

    class SQL:
        class IpumsETL:
            def __init__(self, sql_file_folder: str):
                self.sql_file_folder = sql_file_folder
                self.extract = f"{self.sql_file_folder}/extract.sql"
                self.transform = f"{self.sql_file_folder}/transform.sql"
                self.load = f"{self.sql_file_folder}/load.sql"

        class IpumsTimeConsistentCluster:
            def __init__(self, sql_file_folder: str):
                self.sql_file_folder = sql_file_folder
                self.create_function__template_usa_raster = f"{self.sql_file_folder}/create_function__template_usa_raster.sql"
                self.create_cluster = f"{self.sql_file_folder}/create_cluster.sql"
                self.rasterize_census_places = f"{self.sql_file_folder}/create_table__rasterized_census_places.sql"
                self.create_cluster_intersection_matching = f"{self.sql_file_folder}/create_table__cluster_intersection_matching.sql"
                self.create_time_consistent_cluster = f"{self.sql_file_folder}/create_time_consistent_cluster.sql"

        def __init__(self, sql_file_folder: str):
            self.sql_file_folder = sql_file_folder
            self.ipums_etl = self.IpumsETL(sql_file_folder=f'{self.sql_file_folder}/ipums_etl')
            self.ipums_tcc = self.IpumsTimeConsistentCluster(sql_file_folder=f'{self.sql_file_folder}/ipums_time_consistent_cluster')

    def __init__(self, project_path: str = _project_path, data_folder: str = _data_folder, docker_data_folder: str = _docker_data_folder):
        self.project_path = project_path
        self.source_data = self.Data(data_folder=data_folder, docker_data_folder=docker_data_folder)
        self.sql = self.SQL(sql_file_folder=f"{self.project_path}/src/sql")


class DatabaseInfoManager:
    class TableName:
        def __init__(self):
            self.census_place = "census_place"
            self.industry_code = "industry1950_code"
            self.dem = "dem_{year}"
            self.geo = "geo_{year}"
            self.census = "census_{year}"
            self.census_place_industry_count = "census_place_industry_count_{year}"
            self.cluster = "cluster_{year}"
            self.cluster_industry = "cluster_industry_{year}"
            self.rasterized_census_places = "rasterized_census_places_{year}"
            self.convolved_census_place_raster = "convolved_census_place_raster_{year}"
            self.multiyear_cluster = "multiyear_cluster"
            self.multiyear_census_place_industry_count = "multiyear_census_place_industry_count"
            self.cluster_intersection_matching = "cluster_intersection_matching"
            self.crosswalk_cluster_uid_to_cluster_id = "crosswalk_cluster_uid_to_cluster_id"
            self.time_consistent_cluster = "time_consistent_cluster"
            self.time_consistent_cluster_industry = "time_consistent_cluster_industry"
            self.time_consistent_cluster_geometry = "time_consistent_cluster_geometry"

    def __init__(self, data_folder: str = _data_folder):
        self.temp_duckdb = f"duckdb:///{data_folder}/tmp/temp_duckdb.db"
        self.clusterdb_postgres = 'postgresql+psycopg2://postgres:andrea@localhost:5433/clusterdb'
        self.table_names = self.TableName()


class ParameterManager:
    def __init__(self):
        self.years = [1850, 1860, 1870, 1880, 1900, 1910, 1920, 1930, 1940]
        self.dbscan_eps = 100
        self.dbscan_min_points = 1
        self.pixel_threshold = 100
        self.convolution_kernel_size = 11
        self.convolution_kernel_decay_rate = 0.2


class Config:
    def __init__(self, project_path: str = _project_path, data_folder: str = _data_folder, docker_data_folder: str = _docker_data_folder):
        self.path = PathManager(project_path=project_path, data_folder=data_folder, docker_data_folder=docker_data_folder)
        self.db = DatabaseInfoManager(data_folder=data_folder)
        self.param = ParameterManager()
        self.debug = True


config = Config()