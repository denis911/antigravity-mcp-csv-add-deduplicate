import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from . import csv_ops

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("linkedin-prospecting-csv")

server = Server("linkedin-prospecting-csv")

@server.list_tools()
async def list_tools() -> List[Tool]:
    return [
        Tool(
            name="create_new_csv",
            description="Initialize a fresh, empty CSV file with the Golden Schema headers",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "Full path where the CSV should be created"},
                    "overwrite": {"type": "boolean", "default": False, "description": "If True, will overwrite existing file"}
                },
                "required": ["csv_path"]
            }
        ),
        Tool(
            name="append_profiles_to_csv",
            description="Append new LinkedIn profiles to CSV with auto-normalization and deduplication",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "Absolute path to the CSV file"},
                    "profiles": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "List of profile dictionaries to append (supports legacy field names)"
                    },
                    "dedupe_column": {
                        "type": "string",
                        "default": "linkedin_url",
                        "description": "Column name to use for deduplication"
                    }
                },
                "required": ["csv_path", "profiles"]
            }
        ),
        Tool(
            name="filter_profiles",
            description="Query and filter profiles by multiple criteria using standardized names",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "Absolute path to the CSV file"},
                    "min_score": {"type": "integer"},
                    "max_score": {"type": "integer"},
                    "locations": {"type": "array", "items": {"type": "string"}, "description": "Multi-value location filter"},
                    "companies": {"type": "array", "items": {"type": "string"}, "description": "Multi-value company filter"},
                    "current_role_only": {"type": "boolean"},
                    "found_after_date": {"type": "string", "description": "ISO date string (e.g., '2026-02-16')"},
                    "limit": {"type": "integer"}
                },
                "required": ["csv_path"]
            }
        ),
        Tool(
            name="get_csv_stats",
            description="Get summary statistics about the CSV with Golden Schema support",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "Absolute path to the CSV file"}
                },
                "required": ["csv_path"]
            }
        ),
        Tool(
            name="export_segment",
            description="Export a filtered subset to a new CSV file following Golden Schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_csv": {"type": "string", "description": "Absolute path to source CSV"},
                    "output_csv": {"type": "string", "description": "Absolute path to output CSV"},
                    "min_score": {"type": "integer"},
                    "locations": {"type": "array", "items": {"type": "string"}},
                    "companies": {"type": "array", "items": {"type": "string"}},
                    "columns": {"type": "array", "items": {"type": "string"}, "description": "Columns to include (auto-normalized)"}
                },
                "required": ["source_csv", "output_csv"]
            }
        ),
        Tool(
            name="search_profiles",
            description="Case-insensitive full-text search across all standardized text fields",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "Absolute path to the CSV file"},
                    "search_term": {"type": "string"},
                    "columns": {"type": "array", "items": {"type": "string"}},
                    "case_sensitive": {"type": "boolean", "default": False},
                    "limit": {"type": "integer"}
                },
                "required": ["csv_path", "search_term"]
            }
        ),
        Tool(
            name="deduplicate_csv",
            description="Remove duplicates from CSV using standardized column names",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "Absolute path to the CSV file"},
                    "dedupe_column": {"type": "string", "default": "linkedin_url"},
                    "keep": {"type": "string", "enum": ["first", "last"], "default": "first"}
                },
                "required": ["csv_path"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: Any) -> List[TextContent]:
    try:
        if name == "create_new_csv":
            result = await csv_ops.create_new_csv(**arguments)
        elif name == "append_profiles_to_csv":
            result = await csv_ops.append_profiles_to_csv(**arguments)
        elif name == "filter_profiles":
            result = await csv_ops.filter_profiles(**arguments)
        elif name == "get_csv_stats":
            result = await csv_ops.get_csv_stats(**arguments)
        elif name == "export_segment":
            result = await csv_ops.export_segment(**arguments)
        elif name == "search_profiles":
            result = await csv_ops.search_profiles(**arguments)
        elif name == "deduplicate_csv":
            result = await csv_ops.deduplicate_csv(**arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
            
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    except Exception as e:
        logger.error(f"Error calling tool {name}: {e}")
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

if __name__ == "__main__":
    asyncio.run(main())
