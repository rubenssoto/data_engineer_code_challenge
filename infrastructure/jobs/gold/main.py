from transformations.etl import EtlFactory
from infrastructure.config import ConfigReader
import sys

def main(args):

    config = ConfigReader().read_config(f"jobs/gold/yaml_config/{args}.yaml")

    etl = EtlFactory().get_etl(config=config)

    etl.run()


if __name__ == "__main__":
    main(sys.argv[1])