from infrastructure.config import ConfigReader
from transformations.etl import Etl
from infrastructure.log import setup_logger


def run():
    setup_logger()

    config = ConfigReader().read_config("jobs/silver/yaml_config/table_data.yaml")

    etl = Etl(config=config)

    etl.run()


if __name__ == "__main__":
    run()
