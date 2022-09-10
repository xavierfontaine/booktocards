from typing import Annotated

DictForm = Annotated[str, "Dictionary form of the morpheme"]
Pos = Annotated[str, "Part-of-speech"]
SubPos = Annotated[str, "Sub-compoment of a Sudachi POS"]
Token = Annotated[str, "Token"]
Kanji = Annotated[str, "Kanji"]
Count = Annotated[int, "Count"]
Sentence = Annotated[str, "Sentence"]
SentenceId = Annotated[int, "Sentence id"]
