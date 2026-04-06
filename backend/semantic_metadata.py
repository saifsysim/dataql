"""
Semantic Metadata Layer — the intelligent context layer that transforms
raw schema into business-aware LLM context.

Stores table descriptions, column meanings, business rules, synonyms,
common queries, and access controls in a YAML file that enriches
the LLM prompt beyond raw schema introspection.
"""

from __future__ import annotations
import os
import yaml
from typing import Any, Optional
from pathlib import Path
from pydantic import BaseModel, Field


# ══════════════════════════════════════════════════════════════
#  MODELS
# ══════════════════════════════════════════════════════════════

class ColumnMetadata(BaseModel):
    """Semantic metadata for a single column."""
    description: str = ""
    business_name: str = ""          # Human-friendly name, e.g. "Customer Email"
    sensitivity: str = "public"       # public | internal | restricted | pii
    examples: list[str] = Field(default_factory=list)

class TableMetadata(BaseModel):
    """Semantic metadata for a single table."""
    description: str = ""
    business_name: str = ""          # e.g. "Purchase Orders"
    columns: dict[str, ColumnMetadata] = Field(default_factory=dict)
    business_rules: list[str] = Field(default_factory=list)
    common_queries: list[str] = Field(default_factory=list)
    important_notes: list[str] = Field(default_factory=list)

class SourceMetadata(BaseModel):
    """Semantic metadata for an entire data source."""
    description: str = ""
    tables: dict[str, TableMetadata] = Field(default_factory=dict)
    synonyms: dict[str, str] = Field(default_factory=dict)         # "revenue" → SQL formula
    glossary: dict[str, str] = Field(default_factory=dict)          # "AOV" → "Average Order Value"
    access_roles: dict[str, list[str]] = Field(default_factory=dict)  # role → allowed tables

class SemanticMetadataConfig(BaseModel):
    """Top-level metadata config spanning all data sources."""
    version: str = "1.0"
    organization: str = ""
    sources: dict[str, SourceMetadata] = Field(default_factory=dict)
    global_rules: list[str] = Field(default_factory=list)
    global_synonyms: dict[str, str] = Field(default_factory=dict)


# ══════════════════════════════════════════════════════════════
#  METADATA STORE
# ══════════════════════════════════════════════════════════════

class MetadataStore:
    """
    Loads, saves, and manages the semantic metadata YAML file.
    Provides methods to enrich LLM prompt context with business meaning.
    """

    def __init__(self, metadata_path: str = "metadata.yaml"):
        self.path = Path(metadata_path)
        self.config: SemanticMetadataConfig = self._load()

    def _load(self) -> SemanticMetadataConfig:
        """Load metadata from YAML, or return empty config if not found."""
        if not self.path.exists():
            return SemanticMetadataConfig()
        with open(self.path, "r") as f:
            raw = yaml.safe_load(f) or {}
        return self._parse_raw(raw)

    def _parse_raw(self, raw: dict) -> SemanticMetadataConfig:
        """Parse raw YAML dict into Pydantic models."""
        sources = {}
        for source_id, source_data in raw.get("sources", {}).items():
            tables = {}
            for table_name, table_data in source_data.get("tables", {}).items():
                columns = {}
                for col_name, col_val in table_data.get("columns", {}).items():
                    if isinstance(col_val, str):
                        columns[col_name] = ColumnMetadata(description=col_val)
                    elif isinstance(col_val, dict):
                        columns[col_name] = ColumnMetadata(**col_val)
                tables[table_name] = TableMetadata(
                    description=table_data.get("description", ""),
                    business_name=table_data.get("business_name", ""),
                    columns=columns,
                    business_rules=table_data.get("business_rules", []),
                    common_queries=table_data.get("common_queries", []),
                    important_notes=table_data.get("important_notes", []),
                )
            sources[source_id] = SourceMetadata(
                description=source_data.get("description", ""),
                tables=tables,
                synonyms=source_data.get("synonyms", {}),
                glossary=source_data.get("glossary", {}),
                access_roles=source_data.get("access_roles", {}),
            )

        return SemanticMetadataConfig(
            version=raw.get("version", "1.0"),
            organization=raw.get("organization", ""),
            sources=sources,
            global_rules=raw.get("global_rules", []),
            global_synonyms=raw.get("global_synonyms", {}),
        )

    def save(self):
        """Save current metadata config to YAML."""
        data = {
            "version": self.config.version,
            "organization": self.config.organization,
            "global_rules": self.config.global_rules,
            "global_synonyms": self.config.global_synonyms,
            "sources": {},
        }
        for src_id, src in self.config.sources.items():
            src_data = {
                "description": src.description,
                "tables": {},
                "synonyms": src.synonyms,
                "glossary": src.glossary,
                "access_roles": src.access_roles,
            }
            for tbl_name, tbl in src.tables.items():
                tbl_data = {
                    "description": tbl.description,
                    "business_name": tbl.business_name,
                    "columns": {
                        col_name: col.description
                        for col_name, col in tbl.columns.items()
                    },
                    "business_rules": tbl.business_rules,
                    "common_queries": tbl.common_queries,
                    "important_notes": tbl.important_notes,
                }
                src_data["tables"][tbl_name] = tbl_data
            data["sources"][src_id] = src_data

        with open(self.path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def reload(self):
        """Force-reload from disk."""
        self.config = self._load()

    def update_from_dict(self, raw: dict):
        """Update config from a raw dictionary (API input)."""
        self.config = self._parse_raw(raw)
        self.save()

    # ── Prompt Context Generation ─────────────────────────────

    def enrich_prompt_context(self, raw_schema_context: str) -> str:
        """
        Merge semantic metadata into the raw schema context to produce
        a rich, business-aware prompt for the LLM.
        """
        sections = [raw_schema_context]

        # Global rules
        if self.config.global_rules:
            sections.append("\n═══ GLOBAL BUSINESS RULES ═══")
            for rule in self.config.global_rules:
                sections.append(f"  • {rule}")

        # Global synonyms
        if self.config.global_synonyms:
            sections.append("\n═══ GLOBAL SYNONYMS & DEFINITIONS ═══")
            for term, definition in self.config.global_synonyms.items():
                sections.append(f"  \"{term}\" = {definition}")

        # Per-source metadata
        for source_id, source in self.config.sources.items():
            if source.description:
                sections.append(f"\n═══ SOURCE CONTEXT: {source_id} ═══")
                sections.append(f"  Description: {source.description}")

            # Table descriptions & business rules
            for table_name, table in source.tables.items():
                if table.description or table.columns or table.business_rules:
                    sections.append(f"\n  📋 TABLE: {table_name}")
                    if table.description:
                        sections.append(f"     Description: {table.description}")
                    if table.business_name:
                        sections.append(f"     Also known as: {table.business_name}")

                    # Column descriptions
                    for col_name, col in table.columns.items():
                        if col.description:
                            sections.append(f"     • {col_name}: {col.description}")

                    # Business rules
                    if table.business_rules:
                        sections.append(f"     Business Rules:")
                        for rule in table.business_rules:
                            sections.append(f"       ⚠️  {rule}")

                    # Important notes
                    if table.important_notes:
                        for note in table.important_notes:
                            sections.append(f"     ⚡ {note}")

            # Source synonyms
            if source.synonyms:
                sections.append(f"\n  SYNONYMS for {source_id}:")
                for term, sql in source.synonyms.items():
                    sections.append(f"    \"{term}\" → {sql}")

            # Glossary
            if source.glossary:
                sections.append(f"\n  GLOSSARY for {source_id}:")
                for term, meaning in source.glossary.items():
                    sections.append(f"    {term}: {meaning}")

        return "\n".join(sections)

    def get_metadata_dict(self) -> dict:
        """Return the full metadata as a serializable dict."""
        return self.config.model_dump()

    def get_table_meta(self, source_id: str, table_name: str) -> Optional[TableMetadata]:
        """Lookup metadata for a specific table."""
        source = self.config.sources.get(source_id)
        if source:
            return source.tables.get(table_name)
        return None


# ══════════════════════════════════════════════════════════════
#  LLM-POWERED AUTO-GENERATION
# ══════════════════════════════════════════════════════════════

def auto_generate_metadata(schema_context: str) -> dict:
    """
    Use the LLM to auto-generate semantic metadata from raw schema.
    Returns a raw dict suitable for MetadataStore.update_from_dict().
    """
    import anthropic
    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)

    prompt = f"""Analyze this database schema and generate rich semantic metadata.

{schema_context}

Generate a YAML-compatible JSON response with:
1. A description for each table explaining its business purpose
2. A description for each column explaining what it stores and how it's used
3. Business rules (e.g., "Revenue = total_amount WHERE status != 'cancelled'")
4. Common business synonyms (e.g., "revenue", "AOV", "active customers")
5. A glossary of business terms
6. Common queries users might ask

Respond with ONLY valid JSON in this format:
{{
  "version": "1.0",
  "organization": "Auto-generated",
  "sources": {{
    "sqlite": {{
      "description": "...",
      "tables": {{
        "table_name": {{
          "description": "...",
          "business_name": "...",
          "columns": {{
            "col_name": "description of the column"
          }},
          "business_rules": ["rule 1", "rule 2"],
          "common_queries": ["query 1"],
          "important_notes": ["note 1"]
        }}
      }},
      "synonyms": {{
        "term": "SQL expression or explanation"
      }},
      "glossary": {{
        "term": "plain english definition"
      }}
    }}
  }},
  "global_rules": ["rule 1"],
  "global_synonyms": {{}}
}}"""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response_text = "\n".join(lines)

    import json
    return json.loads(response_text)
