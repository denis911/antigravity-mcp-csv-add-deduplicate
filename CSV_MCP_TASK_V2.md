# MCP Server Task: LinkedIn Prospecting CSV Manager (V2)

## Problem Statement & V1 Feedback
V1 was a great proof-of-concept, but real-world use revealed three major friction points:
1.  **Header Sensitivity:** Slight variations in column names (e.g., "Company Size" vs "company_size") caused `KeyErrors`.
2.  **Pathing Confusion:** The agent occasionally initialized new files instead of appending to existing ones due to relative path resolution.
3.  **Schema Drift:** Managing multiple campaigns requires a "Golden Schema" to ensure consistency across different CSV files.

---

## V2 Requirements

### 1. Unified Naming Convention (Standardized Schema)
All tools must strictly enforce and/or convert headers to the following **snake_case** format. Every tool call should perform an "Auto-Normalization" step: strip whitespace, lowercase everything, and replace spaces/hyphens with underscores.

**The Golden Schema:**
1.  `full_name`
2.  `linkedin_url` (Primary Key for Deduplication)
3.  `headline`
4.  `company`
5.  `company_size`
6.  `location`
7.  `match_score` (Integer)
8.  `match_reason`
9.  `current_role_mention`
10. `found_date` (YYYY-MM-DD)
11. `icp_source`

---

## Tool Definitions

### 1. `append_profiles_to_csv`
**Enhancement:** Must normalize existing headers on load and map incoming "old" keys (like "LinkedIn URL") to the new "Golden Schema" (like `linkedin_url`).
- **Signature:** Unchanged from V1.
- **Added Logic:** If `csv_path` exists, load it and check `df.columns`. If they don't match the Golden Schema, rename them immediately before appending. Return the *actual* total line count from the file.

### 2. `filter_profiles`
**Enhancement:** Support for multi-value filtering (e.g., `locations=["USA", "UK"]`).
- **Signature:** Unchanged from V1.
- **Logic:** Must use normalized column names for internal queries.

### 3. `get_csv_stats`
**Enhancement:** Robust error handling for missing columns.
- **Signature:** Unchanged from V1.
- **Logic:** If a column like `match_score` is missing, return `0` for stats instead of a `KeyError`.

### 4. `export_segment`
- **Signature:** Unchanged from V1.
- **Logic:** Ensure exported CSV follows the Golden Schema.

### 5. `search_profiles`
- **Signature:** Unchanged from V1.
- **Logic:** Perform case-insensitive full-text search across all string columns.

### 6. `deduplicate_csv` (Maintenance)
- **Signature:** Unchanged from V1.
- **Logic:** Standardize on `linkedin_url` as the default dedupe column.

### 7. `create_new_csv` (NEW)
**Purpose:** Initialize a fresh, empty CSV file with the Golden Schema headers to prevent "Guessing" column names in new campaigns.
**Signature:**
```python
async def create_new_csv(
    csv_path: str,
    overwrite: bool = False
) -> Dict[str, str]:
    """
    Creates a new CSV with the Golden Schema headers.
    
    Args:
        csv_path: Full path where the CSV should be created.
        overwrite: If True, will overwrite existing file. If False, will fail if file exists.
    """
```

---

## Developer Guidelines for V2 Implementation

### 1. The "Auto-Repair" Loading Pattern
Every tool that reads a CSV should use this helper logic:
```python
def load_and_normalize(path):
    df = pd.read_csv(path)
    # 1. Clean whitespace from headers
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
    # 2. Map specific common variations
    rename_map = {
        'v2_score': 'match_score',
        'name': 'full_name',
        'linkedin_profile': 'linkedin_url'
    }
    df = df.rename(columns=rename_map)
    return df
```

### 2. Atomic Writes
To prevent data corruption during appends, use a temporary file and `os.replace()` or ensure the file is closed immediately after the pandas `to_csv()` call.

### 3. Absolute Path Enforcement
The server should resolve relative paths against the directory where it was launched to avoid the "7 profiles in a ghost file" issue.

### 4. Better Return Metadata
When appending or filtering, return a `sample_of_added` or `preview` list of 2-3 names so the agent can visually confirm the action was successful without reading the whole file.

---

## Integration with Gemini/Claude
When these tools are available, the agent will:
1.  Check if a CSV exists.
2.  If not, call `create_new_csv`.
3.  Execute search and call `append_profiles_to_csv`.
4.  Call `get_csv_stats` to give the user a progress report.
