import oracledb
from dataclasses import dataclass
from typing import List
import logging

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    data_length: int | None
    nullable: bool


@dataclass
class TableInfo:
    name: str
    owner: str
    columns: List[ColumnInfo]


class MetadataFetcher:
    """Fetch table and column metadata from Oracle"""

    def __init__(self, connection: oracledb.Connection):
        self.conn = connection

    def get_tables(self, owner: str = None) -> List[TableInfo]:
        """Get all tables for a schema"""
        cursor = self.conn.cursor()

        if owner:
            where_clause = "AND owner = :owner"
            params = {"owner": owner.upper()}
        else:
            where_clause = ""
            params = {}

        query = f"""
            SELECT owner, table_name
            FROM all_tables
            WHERE owner = :owner
            ORDER BY table_name
        """

        cursor.execute(query, params)
        rows = cursor.fetchall()

        tables = []
        for owner, table_name in rows:
            tables.append(TableInfo(
                name=table_name,
                owner=owner,
                columns=[]
            ))

        cursor.close()
        return tables

    def get_columns(self, owner: str, table_name: str) -> List[ColumnInfo]:
        """Get columns for a specific table"""
        cursor = self.conn.cursor()

        query = """
            SELECT
                column_name,
                data_type,
                data_length,
                nullable
            FROM all_tab_columns
            WHERE owner = :owner
              AND table_name = :table_name
            ORDER BY column_id
        """

        cursor.execute(query, {
            "owner": owner.upper(),
            "table_name": table_name.upper()
        })

        columns = []
        for col_name, data_type, data_length, nullable in cursor.fetchall():
            columns.append(ColumnInfo(
                name=col_name,
                data_type=data_type,
                data_length=data_length,
                nullable=(nullable == 'Y')
            ))

        cursor.close()
        return columns

    def get_all_tables_with_columns(self, owner: str) -> List[TableInfo]:
        """Get all tables with their columns in one query (more efficient)"""
        cursor = self.conn.cursor()

        query = """
            SELECT
                t.table_name,
                c.column_name,
                c.data_type,
                c.data_length,
                c.nullable
            FROM all_tables t
            JOIN all_tab_columns c
                ON t.owner = c.owner AND t.table_name = c.table_name
            WHERE t.owner = :owner
            ORDER BY t.table_name, c.column_id
        """

        cursor.execute(query, {"owner": owner.upper()})

        tables = {}
        for table_name, col_name, data_type, data_length, nullable in cursor.fetchall():
            if table_name not in tables:
                tables[table_name] = TableInfo(
                    name=table_name,
                    owner=owner,
                    columns=[]
                )

            tables[table_name].columns.append(ColumnInfo(
                name=col_name,
                data_type=data_type,
                data_length=data_length,
                nullable=(nullable == 'Y')
            ))

        cursor.close()
        return list(tables.values())

    def sample_rows(self, owner: str, table_name: str, limit: int = 10) -> List[dict]:
        """Sample random rows from a table for LLM analysis"""
        cursor = self.conn.cursor()

        # Use SAMPLE for random sampling (Oracle feature)
        query = f"""
            SELECT * FROM (
                SELECT * FROM "{owner}"."{table_name}"
                ORDER BY DBMS_RANDOM.VALUE
            ) WHERE ROWNUM <= :limit
        """

        try:
            cursor.execute(query, {"limit": limit})
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            return [dict(zip(columns, row)) for row in rows]
        except oracledb.Error as e:
            logger.warning(f"Cannot sample {owner}.{table_name}: {e}")
            return []
        finally:
            cursor.close()


if __name__ == "__main__":
    print("Metadata fetcher module loaded")