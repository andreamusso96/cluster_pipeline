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

        def __init__(self, sql_file_folder: str):
            self.sql_file_folder = sql_file_folder
            self.ipums_etl = self.IpumsETL(sql_file_folder=f'{self.sql_file_folder}/ipums_etl')

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

    def __init__(self, data_folder: str = _data_folder):
        self.temp_duckdb = f"duckdb:///{data_folder}/tmp/temp.db"
        self.clusterdb_postgres = 'postgresql+psycopg2://postgres:andrea@localhost:5433/clusterdb'
        self.table_names = self.TableName()


class ParameterManager:
    def __init__(self):
        self.years = [1850, 1860]


class Config:
    def __init__(self, project_path: str = _project_path, data_folder: str = _data_folder, docker_data_folder: str = _docker_data_folder):
        self.path = PathManager(project_path=project_path, data_folder=data_folder, docker_data_folder=docker_data_folder)
        self.db = DatabaseInfoManager(data_folder=data_folder)
        self.param = ParameterManager()
        self.log_level = 'DEBUG'


config = Config()