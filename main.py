from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.exporter import export
from src.clean_manifest_errors import clean_manifest_errors
from src.logger import logger
from src.utils import SAS_ENDPOINT

def main():
    logger.info("*" * 50)
    export()

if __name__ == "__main__":
    main()
