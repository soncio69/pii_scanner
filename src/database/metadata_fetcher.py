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
    pk_fk_columns: set = None

    def __post_init__(self):
        if self.pk_fk_columns is None:
            self.pk_fk_columns = set()


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

        # Get PK/FK columns and add to tables
        # This is needed to filter out numeric ID columns that are keys
        pk_fk_columns = self._get_pk_fk_columns(owner)
        for table in tables.values():
            table.pk_fk_columns = pk_fk_columns.get(table.name, set())

        return list(tables.values())

    def _get_pk_fk_columns(self, owner: str) -> dict:
        """
        Get columns that are part of primary or foreign keys.

        Returns:
            Dict mapping table_name -> set of column names that are PK or FK
        """
        cursor = self.conn.cursor()

        # Get primary key columns
        pk_query = """
            SELECT ac.table_name, acc.column_name
            FROM all_constraints ac
            JOIN all_cons_columns acc
                ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
            WHERE ac.owner = :owner
                AND ac.constraint_type = 'P'
        """

        # Get foreign key columns
        fk_query = """
            SELECT ac.table_name, acc.column_name
            FROM all_constraints ac
            JOIN all_cons_columns acc
                ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
            WHERE ac.owner = :owner
                AND ac.constraint_type = 'R'
        """

        result = {}

        try:
            cursor.execute(pk_query, {"owner": owner.upper()})
            for table_name, col_name in cursor.fetchall():
                if table_name not in result:
                    result[table_name] = set()
                result[table_name].add(col_name)

            cursor.execute(fk_query, {"owner": owner.upper()})
            for table_name, col_name in cursor.fetchall():
                if table_name not in result:
                    result[table_name] = set()
                result[table_name].add(col_name)
        except oracledb.Error as e:
            logger.warning(f"Cannot get PK/FK columns: {str(e)}")
        finally:
            cursor.close()

        return result

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
            logger.warning(f"Cannot sample {owner}.{table_name}: {str(e)}", exc_info=True)
            return []
        finally:
            cursor.close()


if __name__ == "__main__":
    print("Metadata fetcher module loaded")