from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.exporter import SasExporter
from src.logger import logger
from src.utils import SAS_ENDPOINT

def main():
    logger.info("*" * 50)
    logger.info(f"Exporting data from '{SAS_ENDPOINT}'")
    SasExporter().pipeline()

if __name__ == "__main__":
    main()
