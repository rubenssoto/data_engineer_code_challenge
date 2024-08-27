from transformations.etl import EtlFactory
from infrastructure.config import ConfigReader
from infrastructure.log import setup_logger
import sys

def main(args):

    setup_logger()

    config = ConfigReader().read_config(f"jobs/gold/yaml_config/{args}.yaml")

    etl = EtlFactory().get_etl(config=config)

    etl.run()


if __name__ == "__main__":
    main(sys.argv[1])