from typing import Union

import pandas as pd
from st_aggrid import AgGrid, AgGridReturn, GridOptionsBuilder

from booktocards.annotations import Kanji, SourceName, Token
from booktocards.kb import ColumnName


def make_ag(df: pd.DataFrame) -> AgGridReturn:
    """Make an ag grid from a DataFrame"""
    grid_option_builder = GridOptionsBuilder.from_dataframe(df)
    grid_option_builder.configure_selection(
        selection_mode="multiple",
        use_checkbox=True,
    )
    grid_options = grid_option_builder.build()
    ag_obj = AgGrid(
        df,
        enable_enterprise_modules=False,
        gridOptions=grid_options,
    )
    return ag_obj


def extract_item_and_source_from_ag(
    ag_grid_output: AgGridReturn,
    item_colname: str,
) -> list[tuple[Union[Token, Kanji], SourceName]]:
    """Extract (item value, source name) info from selected table rows"""
    if item_colname not in [ColumnName.TOKEN, ColumnName.KANJI]:
        raise ValueError(
            f"item_colname must be one of {ColumnName.TOKEN}, {ColumnName.KANJI}"
        )
    item_source_couples = []
    for select_row in ag_grid_output.selected_rows:
        couple = (
            select_row[item_colname],
            select_row[ColumnName.SOURCE_NAME],
        )
        item_source_couples.append(couple)
    return item_source_couples
