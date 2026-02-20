# MCP Server Task: LinkedIn Prospecting CSV Manager

## Problem Statement

Currently, Claude struggles with CSV operations for the LinkedIn prospecting workflow:

1. **Token inefficiency**: Reading a 1000-line CSV consumes 20-30K tokens just to load it into context
2. **Error-prone operations**: Multiple failed attempts at append/deduplicate operations
3. **Data corruption risk**: Manual CSV writing with escaping issues
4. **Poor scalability**: O(n) deduplication scans, full file rewrites for every append
5. **Column mismatches**: DictWriter errors when column order changes

### Current Token Usage (Inefficient)
- Session 1: ~39K tokens for 18 profiles
- Session 2: ~25K tokens for 10 profiles
- **At 1000+ lines**: ~30K tokens just to read the CSV per operation

### Target Token Usage (with MCP)
- CSV operations: ~500 tokens (function call + results only)
- **60x more efficient at scale**

---

## Task: Build MCP Server for CSV Operations

Create a local MCP server that handles all CSV operations efficiently using Python + pandas.

### Server Configuration
- **Name**: `linkedin-prospecting-csv`
- **Transport**: STDIO (local PC)
- **Language**: Python 3.10+
- **Dependencies**: pandas, pathlib

---

## Required Tools

### 1. `append_profiles_to_csv`

**Purpose**: Append new LinkedIn profiles to CSV with automatic deduplication

**Signature**:
```python
async def append_profiles_to_csv(
    csv_path: str,
    profiles: List[Dict[str, Any]],
    dedupe_column: str = "LinkedIn URL"
) -> Dict[str, int]:
    """
    Append new profiles to CSV, removing duplicates based on dedupe_column.
    
    Args:
        csv_path: Path to the CSV file
        profiles: List of profile dictionaries to append
        dedupe_column: Column name to use for deduplication (default: "LinkedIn URL")
    
    Returns:
        {
            "added": number of new profiles added,
            "skipped_duplicates": number of duplicates found and skipped,
            "total_profiles": total profiles in CSV after operation
        }
    """
```

**Implementation Notes**:
- Read existing CSV with pandas
- Concat new profiles
- Use `drop_duplicates(subset=[dedupe_column], keep='first')` to deduplicate
- Write back to CSV with `index=False`
- Handle case where CSV doesn't exist (create new)
- Preserve column order from original CSV

**Example Usage**:
```json
{
  "csv_path": "C:\\path\\to\\profiles.csv",
  "profiles": [
    {
      "Name": "John Doe",
      "LinkedIn URL": "https://linkedin.com/in/johndoe",
      "Headline": "Senior Data Engineer",
      "Company": "Meta",
      "Company Size": "77000+",
      "Location": "USA",
      "v2 Score": "22",
      "Match Reason": "Senior(+2) Tools-Scrapy(+5)...",
      "CURRENT Role Mention": "YES - builds data pipelines",
      "Found Date": "2026-02-18",
      "ICP Source": "ICP_v2.md"
    }
  ],
  "dedupe_column": "LinkedIn URL"
}
```

**Expected Output**:
```json
{
  "added": 1,
  "skipped_duplicates": 0,
  "total_profiles": 55
}
```

---

### 2. `filter_profiles`

**Purpose**: Query and filter profiles by multiple criteria

**Signature**:
```python
async def filter_profiles(
    csv_path: str,
    min_score: Optional[int] = None,
    max_score: Optional[int] = None,
    locations: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    current_role_only: Optional[bool] = None,
    found_after_date: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Filter profiles based on criteria.
    
    Args:
        csv_path: Path to the CSV file
        min_score: Minimum v2 Score (inclusive)
        max_score: Maximum v2 Score (inclusive)
        locations: List of locations to include (e.g., ["USA", "Canada"])
        companies: List of companies to include
        current_role_only: If True, only return profiles with "YES" in CURRENT Role Mention
        found_after_date: ISO date string (e.g., "2026-02-16")
        limit: Maximum number of results to return
    
    Returns:
        List of profile dictionaries matching criteria
    """
```

**Implementation Notes**:
- Use pandas query/filter operations
- Convert v2 Score to int for comparison
- Use `str.contains()` for partial location matching (e.g., "USA" matches "USA (Seattle)")
- For current_role_only, check if "CURRENT Role Mention" starts with "YES"
- Sort by v2 Score descending before applying limit

**Example Usage**:
```json
{
  "csv_path": "C:\\path\\to\\profiles.csv",
  "min_score": 20,
  "locations": ["USA", "Canada"],
  "current_role_only": true,
  "limit": 10
}
```

**Expected Output**:
```json
[
  {
    "Name": "Alexander Wang",
    "LinkedIn URL": "https://ca.linkedin.com/in/alexwangdata",
    "v2 Score": "25",
    ...
  },
  ...
]
```

---

### 3. `get_csv_stats`

**Purpose**: Get summary statistics about the CSV

**Signature**:
```python
async def get_csv_stats(
    csv_path: str
) -> Dict[str, Any]:
    """
    Get statistics about profiles in CSV.
    
    Args:
        csv_path: Path to the CSV file
    
    Returns:
        {
            "total_profiles": int,
            "avg_score": float,
            "score_distribution": {
                "20+": int,
                "15-19": int,
                "10-14": int,
                "<10": int
            },
            "location_breakdown": {"USA": 18, "Canada": 7, ...},
            "company_size_breakdown": {"Large": 32, "Unknown": 22, ...},
            "found_date_range": {"earliest": "2026-02-11", "latest": "2026-02-18"},
            "current_role_count": int (number with "YES" in current role)
        }
    """
```

**Implementation Notes**:
- Convert v2 Score to numeric for calculations
- Use `value_counts()` for breakdowns
- Calculate percentages for location/company size
- Handle missing/invalid scores gracefully

---

### 4. `export_segment`

**Purpose**: Export a filtered subset to a new CSV file

**Signature**:
```python
async def export_segment(
    source_csv: str,
    output_csv: str,
    min_score: Optional[int] = None,
    locations: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Export filtered profiles to new CSV file.
    
    Args:
        source_csv: Path to source CSV file
        output_csv: Path to output CSV file
        min_score: Minimum v2 Score filter
        locations: Location filter
        companies: Company filter
        columns: List of columns to include (None = all columns)
    
    Returns:
        {
            "profiles_exported": int,
            "output_path": str,
            "columns_included": List[str]
        }
    """
```

**Implementation Notes**:
- Use same filtering logic as `filter_profiles`
- If columns specified, select only those columns
- Write to output_csv
- Return confirmation with counts

**Example Usage** (CRM Export):
```json
{
  "source_csv": "C:\\path\\to\\profiles.csv",
  "output_csv": "C:\\path\\to\\crm_import_usa_tier1.csv",
  "min_score": 20,
  "locations": ["USA"],
  "columns": ["Name", "LinkedIn URL", "Headline", "Company", "v2 Score"]
}
```

---

### 5. `search_profiles`

**Purpose**: Full-text search across all text fields

**Signature**:
```python
async def search_profiles(
    csv_path: str,
    search_term: str,
    columns: Optional[List[str]] = None,
    case_sensitive: bool = False,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Search for profiles containing search term.
    
    Args:
        csv_path: Path to CSV file
        search_term: Text to search for
        columns: Columns to search in (None = all text columns)
        case_sensitive: Whether search is case-sensitive
        limit: Max results
    
    Returns:
        List of matching profiles
    """
```

**Implementation Notes**:
- Search in: Headline, Company, Match Reason, CURRENT Role Mention
- Use pandas str.contains() with regex=False
- Highlight matching column in results if possible

**Example Usage**:
```json
{
  "csv_path": "C:\\path\\to\\profiles.csv",
  "search_term": "Scrapy",
  "columns": ["Headline", "Match Reason"],
  "limit": 20
}
```

---

### 6. `deduplicate_csv`

**Purpose**: Remove all duplicates from CSV (maintenance operation)

**Signature**:
```python
async def deduplicate_csv(
    csv_path: str,
    dedupe_column: str = "LinkedIn URL",
    keep: str = "first"
) -> Dict[str, int]:
    """
    Remove duplicates from CSV file.
    
    Args:
        csv_path: Path to CSV file
        dedupe_column: Column to use for deduplication
        keep: Which duplicate to keep ('first' or 'last')
    
    Returns:
        {
            "original_count": int,
            "duplicates_removed": int,
            "final_count": int
        }
    """
```

**Implementation Notes**:
- Read CSV
- Count before deduplication
- Use `drop_duplicates(subset=[dedupe_column], keep=keep)`
- Write back to same file
- Return counts

---

## Server Structure

```
antigravity-mcp-csv-add-deduplicate/
├── pyproject.toml          # Project dependencies
├── src/
│   └── linkedin_prospecting_csv/
│       ├── __init__.py
│       ├── server.py       # Main MCP server
│       └── csv_ops.py      # CSV operation functions
└── README.md
```

### pyproject.toml
```toml
[project]
name = "linkedin-prospecting-csv"
version = "0.1.0"
description = "MCP server for LinkedIn prospecting CSV operations"
dependencies = [
    "mcp>=0.9.0",
    "pandas>=2.0.0"
]
requires-python = ">=3.10"

[project.scripts]
linkedin-prospecting-csv = "linkedin_prospecting_csv.server:main"
```

### server.py Structure
```python
from mcp.server import Server
from mcp.types import Tool, TextContent
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional

server = Server("linkedin-prospecting-csv")

@server.list_tools()
async def list_tools() -> List[Tool]:
    return [
        Tool(
            name="append_profiles_to_csv",
            description="Append new LinkedIn profiles to CSV with deduplication",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string"},
                    "profiles": {"type": "array", "items": {"type": "object"}},
                    "dedupe_column": {"type": "string", "default": "LinkedIn URL"}
                },
                "required": ["csv_path", "profiles"]
            }
        ),
        # ... other tools
    ]

@server.call_tool()
async def call_tool(name: str, arguments: Any) -> List[TextContent]:
    if name == "append_profiles_to_csv":
        result = await append_profiles_to_csv(**arguments)
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    # ... handle other tools

# Implement tool functions
async def append_profiles_to_csv(
    csv_path: str,
    profiles: List[Dict],
    dedupe_column: str = "LinkedIn URL"
) -> Dict:
    # Implementation here
    pass

async def main():
    from mcp.server.stdio import stdio_server
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

---

## Testing Requirements

Create test cases for each tool:

### Test Data (test_profiles.csv)
```csv
Name,LinkedIn URL,Headline,Company,Company Size,Location,v2 Score,Match Reason,CURRENT Role Mention,Found Date,ICP Source
John Doe,https://linkedin.com/in/johndoe,Senior Data Engineer,Meta,77000+,USA,22,"Senior(+2) Tools(+5)",YES - current work,2026-02-18,ICP_v2.md
Jane Smith,https://linkedin.com/in/janesmith,Data Engineer,Google,100000+,USA,18,"Tools(+5)",YES - current work,2026-02-18,ICP_v2.md
```

### Test Cases

**Test 1: Append with no duplicates**
```python
profiles = [{"Name": "Bob Jones", "LinkedIn URL": "https://linkedin.com/in/bobjones", ...}]
result = await append_profiles_to_csv("test.csv", profiles)
assert result["added"] == 1
assert result["skipped_duplicates"] == 0
```

**Test 2: Append with duplicates**
```python
profiles = [{"Name": "John Doe", "LinkedIn URL": "https://linkedin.com/in/johndoe", ...}]
result = await append_profiles_to_csv("test.csv", profiles)
assert result["added"] == 0
assert result["skipped_duplicates"] == 1
```

**Test 3: Filter by score**
```python
results = await filter_profiles("test.csv", min_score=20)
assert len(results) == 1
assert results[0]["Name"] == "John Doe"
```

**Test 4: Get stats**
```python
stats = await get_csv_stats("test.csv")
assert stats["total_profiles"] == 2
assert stats["avg_score"] > 15
```

---

## Claude Desktop Configuration

After building the server, add to Claude Desktop config:

**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "linkedin-prospecting-csv": {
      "command": "python",
      "args": [
        "-m",
        "linkedin_prospecting_csv.server"
      ],
      "env": {}
    }
  }
}
```

Or using uvx:
```json
{
  "mcpServers": {
    "linkedin-prospecting-csv": {
      "command": "uvx",
      "args": [
        "linkedin-prospecting-csv"
      ]
    }
  }
}
```

---

## Expected Benefits

### Token Efficiency
- **Before**: 30K tokens to read 1000-line CSV
- **After**: 500 tokens per operation (just function call + result)
- **Savings**: 60x reduction

### Speed
- **Before**: Multiple failed attempts, full file rewrites
- **After**: Instant pandas operations, atomic writes

### Reliability
- **Before**: Column mismatches, escaping errors, data corruption risk
- **After**: Pandas handles all edge cases automatically

### Scalability
- **Before**: O(n) scans, unworkable at 5000+ lines
- **After**: O(1) operations, handles 100K+ lines easily

---

## Implementation Checklist

- [ ] Set up Python project structure
- [ ] Implement `append_profiles_to_csv` with deduplication
- [ ] Implement `filter_profiles` with multi-criteria filtering
- [ ] Implement `get_csv_stats` with breakdowns
- [ ] Implement `export_segment` for CRM exports
- [ ] Implement `search_profiles` for full-text search
- [ ] Implement `deduplicate_csv` for maintenance
- [ ] Write unit tests for all functions
- [ ] Test with real CSV file (54 profiles)
- [ ] Add error handling (file not found, invalid CSV, etc.)
- [ ] Add logging for debugging
- [ ] Create README with usage examples
- [ ] Package for distribution (optional)
- [ ] Add to Claude Desktop config
- [ ] Test integration with Claude

---

## Success Criteria

✅ All 6 tools working correctly  
✅ No data corruption or loss  
✅ Deduplication is accurate (no false positives/negatives)  
✅ Filtering works with multiple criteria  
✅ Stats are accurate  
✅ Token usage reduced by 50x+  
✅ Operations complete in <1 second for 1000 rows  
✅ Can handle 10,000+ rows without issues  

---

## Future Enhancements (Optional)

1. **Bulk operations**: `append_from_multiple_csvs`, `merge_csvs`
2. **Validation**: Validate profile data against schema before appending
3. **Backup**: Auto-backup CSV before destructive operations
4. **Enrichment**: Add tool to enrich profiles (e.g., calculate derived fields)
5. **Analytics**: More advanced stats (cohort analysis, trend over time)
6. **Export formats**: Support JSON, Excel, Google Sheets

---

## Notes for Agentic Coding System

- Use **pandas** - it's the right tool for this job
- Keep functions **async** for MCP compatibility
- Add **type hints** for all parameters
- Include **docstrings** with examples
- Handle **edge cases** (empty CSV, malformed data, missing columns)
- Make operations **atomic** (don't leave CSV in corrupted state)
- Add **logging** for debugging
- Return **structured data** (dicts, not strings)
- Test with **real-world data** (the actual 54-profile CSV)

This MCP server will transform the LinkedIn prospecting workflow from token-heavy and error-prone to efficient and reliable. Python + pandas is the right solution for CSV operations at scale.
