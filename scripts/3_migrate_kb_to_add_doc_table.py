"""
2026/01/22: migrate kb to add doc table
"""

import pandas as pd

from booktocards.kb import DATA_MODEL, ColumnName, KnowledgeBase, TableName

kb = KnowledgeBase()

# Load the tables manually. This is because one table is missing -> the init assumes
# there is no db and re-initializes all tables.
for table_name in DATA_MODEL.keys():
    if table_name != TableName.DOCS:
        kb._load_df(df_name=table_name)

# Create table
kb.__dict__[TableName.DOCS] = pd.DataFrame(columns=DATA_MODEL[TableName.DOCS].keys()).astype(DATA_MODEL[TableName.DOCS])  # type: ignore [call-overload]

# Add entries
document_names = kb[TableName.TOKENS][ColumnName.SOURCE_NAME].unique()
for doc_name in document_names:
    kb.create_source_entry(source_name=doc_name, hide_in_add_full_doc_app=False)

# Save
kb.save_kb(make_backup=True)
