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
            name="append_profiles_to_csv",
            description="Append new LinkedIn profiles to CSV with automatic deduplication",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "Path to the CSV file"},
                    "profiles": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "List of profile dictionaries to append"
                    },
                    "dedupe_column": {
                        "type": "string",
                        "default": "LinkedIn URL",
                        "description": "Column name to use for deduplication"
                    }
                },
                "required": ["csv_path", "profiles"]
            }
        ),
        Tool(
            name="filter_profiles",
            description="Query and filter profiles by multiple criteria",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string"},
                    "min_score": {"type": "integer"},
                    "max_score": {"type": "integer"},
                    "locations": {"type": "array", "items": {"type": "string"}},
                    "companies": {"type": "array", "items": {"type": "string"}},
                    "current_role_only": {"type": "boolean"},
                    "found_after_date": {"type": "string", "description": "ISO date string (e.g., '2026-02-16')"},
                    "limit": {"type": "integer"}
                },
                "required": ["csv_path"]
            }
        ),
        Tool(
            name="get_csv_stats",
            description="Get summary statistics about the CSV",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string"}
                },
                "required": ["csv_path"]
            }
        ),
        Tool(
            name="export_segment",
            description="Export a filtered subset to a new CSV file",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_csv": {"type": "string"},
                    "output_csv": {"type": "string"},
                    "min_score": {"type": "integer"},
                    "locations": {"type": "array", "items": {"type": "string"}},
                    "companies": {"type": "array", "items": {"type": "string"}},
                    "columns": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["source_csv", "output_csv"]
            }
        ),
        Tool(
            name="search_profiles",
            description="Full-text search across text fields like Headline, Company, Match Reason",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string"},
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
            description="Remove all duplicates from CSV (maintenance operation)",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string"},
                    "dedupe_column": {"type": "string", "default": "LinkedIn URL"},
                    "keep": {"type": "string", "enum": ["first", "last"], "default": "first"}
                },
                "required": ["csv_path"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: Any) -> List[TextContent]:
    try:
        if name == "append_profiles_to_csv":
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
