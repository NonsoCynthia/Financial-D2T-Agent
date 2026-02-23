from enum import Enum
from typing_extensions import TypedDict


class QueryInput(TypedDict):
    sql_query: str


class ResponseFormat(Enum):
    MARKDOWN = "markdown"
    DICT = "dict"


def _create_markdown_table(rows: list, columns: list[str]) -> str:
    output = "| " + " | ".join(columns) + " |\n"
    output += "| " + " | ".join(["---" for _ in columns]) + " |\n"
    for row in rows:
        output += "| " + " | ".join([str(cell) if cell is not None else "" for cell in row]) + " |\n"
    return output


def run_sql_query(
    inp: QueryInput,
    db_path: str,
    response_format: ResponseFormat = ResponseFormat.MARKDOWN,
) -> dict:
    import sqlite3

    try:
        sql_query = inp["sql_query"]

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)

            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows = cursor.fetchall()

            if not rows:
                return {"status": "success", "report": "No data found with the given query"}

            if response_format == ResponseFormat.MARKDOWN:
                output = _create_markdown_table(rows=rows, columns=columns)
            elif response_format == ResponseFormat.DICT:
                output = [dict(zip(columns, row)) for row in rows]
            else:
                output = rows

            return {"status": "success", "report": output}

    except Exception as e:
        return {"status": "error", "report": f"Failed to execute query: {e}"}
