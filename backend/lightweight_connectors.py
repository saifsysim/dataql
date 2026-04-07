"""
Lightweight DataQL Connectors — re-export shim.
Each connector now lives in its own file; this module re-exports them
for backward compatibility.
"""

from csv_connector import CSVConnector
from json_connector import JSONConnector
from config_connector import EnvConfigConnector
from notion_connector import NotionConnector
from airtable_connector import AirtableConnector
from github_connector import GitHubIssuesConnector

__all__ = [
    "CSVConnector",
    "JSONConnector",
    "EnvConfigConnector",
    "NotionConnector",
    "AirtableConnector",
    "GitHubIssuesConnector",
]
