"""Schema introspector — reads database structure and formats it as LLM context.
Supports multi-connector registry: introspects all registered connectors."""

from __future__ import annotations
from models import SchemaInfo, TableInfo, ColumnInfo
from data_connectors import BaseConnector, ConnectorRegistry


def introspect_schema(connector: BaseConnector) -> SchemaInfo:
    """Read the full database schema for a single connector."""
    tables = []
    table_names = connector.get_tables()

    for table_name in table_names:
        columns_raw = connector.get_table_info(table_name)
        foreign_keys = connector.get_foreign_keys(table_name)

        fk_lookup = {
            fk["from_column"]: f"{fk['to_table']}.{fk['to_column']}"
            for fk in foreign_keys
        }

        columns = []
        for col in columns_raw:
            columns.append(ColumnInfo(
                name=col["name"],
                data_type=col["data_type"],
                nullable=col["nullable"],
                is_primary_key=col["is_primary_key"],
                is_foreign_key=col["name"] in fk_lookup,
                references=fk_lookup.get(col["name"]),
            ))

        row_count = connector.get_row_count(table_name)
        tables.append(TableInfo(
            name=table_name,
            columns=columns,
            row_count=row_count,
        ))

    return SchemaInfo(
        database_type=getattr(connector, 'connector_name', 'unknown'),
        tables=tables,
    )


def introspect_all(registry: ConnectorRegistry) -> list[dict]:
    """Introspect all registered connectors and return a list of source schemas."""
    results = []
    for connector in registry.get_all():
        try:
            schema = introspect_schema(connector)
            results.append({
                "source_id": connector.source_id,
                "name": connector.connector_name,
                "icon": connector.connector_icon,
                "type": connector.connector_type,
                "schema": schema,
            })
        except Exception as e:
            results.append({
                "source_id": connector.source_id,
                "name": connector.connector_name,
                "icon": connector.connector_icon,
                "type": connector.connector_type,
                "schema": SchemaInfo(database_type=connector.connector_name, tables=[]),
                "error": str(e),
            })
    return results


def all_schemas_to_prompt_context(sources: list[dict]) -> str:
    """Convert all source schemas into a single compact LLM context string."""
    lines = ["AVAILABLE DATA SOURCES:", ""]

    for source in sources:
        schema = source["schema"]
        lines.append(f"═══ {source['icon']} {source['name']} (source: {source['source_id']}) ═══")
        if not schema.tables:
            lines.append("  (no tables available)")
            lines.append("")
            continue

        for table in schema.tables:
            lines.append(f"  TABLE: {table.name} ({table.row_count} rows)")
            for col in table.columns:
                markers = []
                if col.is_primary_key:
                    markers.append("PK")
                if col.is_foreign_key:
                    markers.append(f"FK → {col.references}")
                marker_str = f"  [{', '.join(markers)}]" if markers else ""
                nullable = " (nullable)" if col.nullable and not col.is_primary_key else ""
                lines.append(f"    - {col.name}: {col.data_type}{nullable}{marker_str}")
        lines.append("")

    return "\n".join(lines)


# Backward compat
def schema_to_prompt_context(schema: SchemaInfo) -> str:
    """Convert a single SchemaInfo into a compact text block for the LLM."""
    lines = [f"DATABASE TYPE: {schema.database_type}", ""]
    for table in schema.tables:
        lines.append(f"TABLE: {table.name} ({table.row_count} rows)")
        for col in table.columns:
            markers = []
            if col.is_primary_key:
                markers.append("PK")
            if col.is_foreign_key:
                markers.append(f"FK → {col.references}")
            marker_str = f"  [{', '.join(markers)}]" if markers else ""
            nullable = " (nullable)" if col.nullable and not col.is_primary_key else ""
            lines.append(f"  - {col.name}: {col.data_type}{nullable}{marker_str}")
        lines.append("")
    return "\n".join(lines)
