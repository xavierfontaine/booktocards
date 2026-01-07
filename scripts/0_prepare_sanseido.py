import logging
import os

import deepl

from booktocards import io, jj_dicts

# =========
# Constants
# =========
# Name of folders from data/in/dictionaries
SANSEIDO_IN_FOLDER_NAME = "三省堂　スーパー大辞林"
SANSEIDO_OUT_FOLDER_NAME = "dictionaries"
SANSEIDO_OUT_FILENAME = "sanseido_dict.pickle"


# ======
# Logger
# ======
logging.basicConfig(
    format="[%(levelname)s] %(asctime)s %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)


# ================
# Prepare sanseido
# ================
# Path to raw files for the sanseido dict
in_dirpath = io.get_path_to_dict(dict_name=SANSEIDO_IN_FOLDER_NAME)
# Prepare the dict
logger.info(f"Prepare sanseido from raw files in {in_dirpath}")
manip_sanseido = jj_dicts.ManipulateSanseido()
manip_sanseido.from_raw_files(dirpath=in_dirpath)
# Save to pickle
out_dirpath = os.path.join(
    io.get_data_path(),
    "out",
    SANSEIDO_OUT_FOLDER_NAME,
)
if not os.path.isdir(out_dirpath):
    os.mkdir(path=out_dirpath)
out_filepath = os.path.join(out_dirpath, SANSEIDO_OUT_FILENAME)
logger.info(f"Save sanseido to pickle in {out_filepath}")
manip_sanseido.to_json(filepath=out_filepath)
