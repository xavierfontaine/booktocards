import deepl
import logging
import os

from booktocards import jj_dicts, io

# =========
# Constants
# =========
# Keys in conf/secrets.yaml
SECRETS_DEEPL_KEY_KEY = "deepl_api_key"
# Sanseido dict
SANSEIDO_PICKLE_FOLDER_NAME = "dictionnaries"
SANSEIDO_PICKLE_FILENAME = "sanseido_dict.pickle"


# ======
# Logger
# ======
logging.basicConfig(
    format="[%(levelname)s] %(asctime)s %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)


# ============
# Load secrets
# ============
secrets = io.get_secrets()


# =============
# Load sanseido
# =============
# Get path
in_filepath = os.path.join(
    io.get_data_path(),
    "out",
    SANSEIDO_PICKLE_FOLDER_NAME,
    SANSEIDO_PICKLE_FILENAME,
)
# Load
logger.info("Load sanseido from pickle")
manip_sanseido = jj_dicts.ManipulateSanseido()
manip_sanseido.from_pickle(filepath=in_filepath)
sanseido_dict = manip_sanseido.sanseido_dict


# TODO : remove
exit()


# =========================
# Perform dummy translation
# =========================
# Set up translator with API key
translator = deepl.Translator(secrets[SECRETS_DEEPL_KEY_KEY])
# Translate sequence
source_seq = "僕は忍者だ。"
result = translator.translate_text(
    text=source_seq, source_lang="JA", target_lang="EN-US"
)
print(result.text)
