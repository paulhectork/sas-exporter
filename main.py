from dotenv import load_dotenv
load_dotenv()  # NOTE: necessary to load .env before importing variables relying on the env !

from src.utils import LOG_DIR # load_save, set_paths
from src.logger import logger

logger.info("HELLO !")

# sanity_check()  # check all .env variablers are defined
# set_paths()     # define output and log paths
# make_paths()    # create output paths if necessary
# load_save()     # load a "save" of all correctly downloaded files
