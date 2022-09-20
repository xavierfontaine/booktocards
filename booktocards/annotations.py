from typing import Annotated

Count = Annotated[int, "Count"]
Definition = Annotated[str, "Definition"]
DictForm = Annotated[str, "Dictionary form of the morpheme"]
DictEntry = Annotated[any, "Dictionary entry (content depends on dictionary)"]
Kanji = Annotated[str, "Kanji"]
Pos = Annotated[str, "Part-of-speech"]
Reading = Annotated[str, "Reading"]
Sentence = Annotated[str, "Sentence"]
SentenceId = Annotated[int, "Sentence id"]
SubPos = Annotated[str, "Sub-compoment of a Sudachi POS"]
Token = Annotated[str, "Token"]
