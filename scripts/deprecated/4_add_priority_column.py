"""
2026/01/31: add priority column to token table
"""

from booktocards.kb import KnowledgeBase, TableName

kb = KnowledgeBase()
kb.__dict__[TableName.TOKENS]["priority"] = 1  # stands for normal priority
# Switch dtype of priority to int8
kb.__dict__[TableName.TOKENS] = kb.__dict__[TableName.TOKENS].astype(
    {"priority": "int8"}
)
kb.save_kb(make_backup=True)
