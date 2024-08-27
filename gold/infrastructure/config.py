from common.readers import AbstractFileReader, FileReaderFactory
from common.writers import AbstractFileWriter, FileWriterFactory
import yaml


class Config:

    def __init__(
        self, file_reader: AbstractFileReader, file_writer: AbstractFileWriter, etl_name: str
    ):
        self.file_reader = file_reader
        self.file_writer = file_writer
        self.etl_name = etl_name


class ConfigReader:

    def read_config(self, config_path: str) -> Config:

        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file)

        file_type = config.get("file_type")
        source_path = config.get("source_path")

        file_reader = FileReaderFactory().get_reader(
            file_type=file_type, source_path=source_path
        )

        write_config = config.get("write_config")

        file_writer = FileWriterFactory().get_writer(write_config=write_config)

        etl_name = config.get("etl_name")

        return Config(file_reader=file_reader, file_writer=file_writer, etl_name=etl_name)
