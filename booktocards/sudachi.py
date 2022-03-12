import sudachipy as sp
from typing import List, Tuple
from booktocards.annotations import DictForm, Pos


# =========
# Functions
# =========
def tokenize(
    doc: str, split_mode: str, dict_name="full"
) -> List[Tuple[DictForm, List[Pos]]]:
    """Return tokenized document as dictionary forms and POS.

    Args:
        doc (str): doc
        split_mode (str): one of "A", "B", "C"
        dict_name: "small", "core" or "full"

    Returns:
        List[Tuple[DictForm, List[Pos]]]:
    """
    split_mode = getattr(sp.SplitMode, split_mode)
    tokenizer = sp.Dictionary(dict=dict_name).create()
    morphemes = tokenizer.tokenize(text=doc, mode=split_mode)
    dictform_pos = [
        (m.dictionary_form(), m.part_of_speech()) for m in morphemes
    ]
    return dictform_pos
