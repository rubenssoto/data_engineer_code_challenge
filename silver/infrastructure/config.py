from common.readers import FileReaderFactory, AbstractFileReader
from transformations.transformers import TransformerFactory, AbstractTransformer
from common.writers import FileWriterFactory, AbstractFileWriter
from infrastructure.checkpoint import Checkpoint
import yaml


class Config:
    """
    Represents the configuration for the data processing pipeline.

    The `Config` class encapsulates the necessary components for the data processing pipeline,
    including the file reader, transformers, file writer, and checkpoint.

    Attributes:
        file_reader (AbstractFileReader): The file reader responsible for reading the input data.
        transformers (list[AbstractTransformer]): The list of transformers to be applied to the data.
        file_writer (AbstractFileWriter): The file writer responsible for writing the processed data.
        checkpoint (Checkpoint): The checkpoint for tracking the processing progress.
    """

    def __init__(
        self,
        file_reader: AbstractFileReader,
        transformers: list[AbstractTransformer],
        file_writer: AbstractFileWriter,
        checkpoint: Checkpoint,
    ):
        self.file_reader = file_reader
        self.transformers = transformers
        self.file_writer = file_writer
        self.checkpoint = checkpoint


class ConfigReader:
    """
    Reads the configuration for the data processing pipeline from a YAML file.

    The `ConfigReader` class is responsible for reading the configuration file and
    creating the necessary components for the data processing pipeline, including
    the file reader, transformers, file writer, and checkpoint.

    Args:
        config_path (str): The path to the configuration file.

    Returns:
        Config: The configuration object for the data processing pipeline.
    """

    def read_config(self, config_path: str) -> Config:

        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file)

        file_type = config.get("file_type")
        source_path = config.get("source_path")

        file_reader = FileReaderFactory().get_reader(
            file_type=file_type, source_path=f"{source_path}/unprocessed/"
        )

        transformers_config = config.get("transformers")
        transformers = list()

        for transformer_config in transformers_config:

            transformers.append(
                TransformerFactory.get_transformer(
                    transformer_config=transformer_config
                )
            )

        write_config = config.get("write_config")

        file_writer = FileWriterFactory().get_writer(write_config=write_config)

        checkpoint = Checkpoint(
            source_path=f"{source_path}/unprocessed/",
            destination_path=f"{source_path}/processed/",
        )

        return Config(
            file_reader=file_reader,
            transformers=transformers,
            file_writer=file_writer,
            checkpoint=checkpoint,
        )
