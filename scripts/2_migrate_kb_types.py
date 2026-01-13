"""
2026/01/12: migrate kb from old types to those specified in DATA_MODEL.
"""

from booktocards.kb import DATA_MODEL, KnowledgeBase

kb = KnowledgeBase()
for df_name in DATA_MODEL.keys():
    kb.__dict__[df_name] = kb.__dict__[df_name].astype(DATA_MODEL[df_name])

kb.save_kb()
