# LinkedIn Prospecting CSV Manager (MCP Server)

A high-performance, token-efficient Model Context Protocol (MCP) server designed for managing LinkedIn prospecting data in local CSV files. Built with Python and `pandas`.

## Why This Project?

Managing large CSV files directly within an LLM (like Claude or ChatGPT) is inefficient and error-prone:
- **Token Drain**: Reading a 1000-line CSV can consume ~30,000 tokens per operation.
- **Data Corruption**: Manual file writing by LLMs often leads to escaping issues or column mismatches.
- **Scalability**: LLMs struggle with O(n) deduplication and full-file rewrites.

**This MCP server reduces token usage by 60x+** by offloading CSV logic to your local machine.

## 🛠️ Features (V2)

- **Standardized Golden Schema**: Enforces a consistent set of columns across all your prospecting campaigns.
- **"Auto-Repair" Header Normalization**: Automatically renames legacy or inconsistent headers (e.g., `v2 Score` -> `match_score`) to match the Golden Schema.
- **Atomic Writes**: Uses temporary-file-and-replace patterns to ensure zero data corruption during file updates.
- **Efficient Appending**: Add new profiles with automatic deduplication based on `linkedin_url`.
- **Multi-Value Filtering**: Query profiles by Score, Company, or multiple Locations (e.g., `["USA", "Canada"]`).
- **Full-Text Search**: Case-insensitive search across all text fields.
- **Absolute Path Enforcement**: Prevents "ghost files" by resolving all paths reliably.

## The Golden Schema

Every CSV processed by the server is automatically standardized to:
1. `full_name`
2. `linkedin_url` (Deduplication Primary Key)
3. `headline`
4. `company`
5. `company_size`
6. `location`
7. `match_score`
8. `match_reason`
9. `current_role_mention`
10. `found_date`
11. `icp_source`

## 📦 Installation

### Prerequisites
- [Python 3.10+](https://www.python.org/downloads/)
- [`uv`](https://github.com/astral-sh/uv) (recommended for dependency management)

### Local Setup
1. Clone this repository:
   ```bash
   git clone https://github.com/denis911/antigravity-mcp-csv-add-deduplicate.git
   cd antigravity-mcp-csv-add-deduplicate
   ```
2. Install dependencies:
   ```bash
   uv sync
   ```

### Claude Desktop Integration

Add the following to your Claude Desktop configuration file:
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

> [!IMPORTANT]
> On Windows, we recommend using the `python -m` syntax below to bypass system security policies (like App Control Policy 4551) that might block the default `uv` executable shims.

```json
{
  "mcpServers": {
    "linkedin-prospecting-csv": {
      "command": "uv",
      "args": [
        "--directory",
        "C:\\path\\to\\your\\repo\\antigravity-mcp-csv-add-deduplicate",
        "run",
        "python",
        "-m",
        "linkedin_prospecting_csv.server"
      ]
    }
  }
}
```

## 🧪 Testing

### Automated Testing
We use `pytest` with real-world data from the `TESTS` directory:
```bash
uv run pytest TESTS/test_csv_ops.py
```

## 🛠️ Available Tools

| Tool | Purpose |
| :--- | :--- |
| `create_new_csv` | Initialize a fresh CSV with Golden Schema headers |
| `append_profiles_to_csv` | Add new profiles + Auto-Repair + Deduplicate |
| `filter_profiles` | Query profiles by criteria (multi-value support) |
| `get_csv_stats` | Summary statistics & breakdowns (Auto-Repair on load) |
| `export_segment` | Save filtered results to new Golden Schema CSV |
| `search_profiles` | Full-text search across standardized columns |
| `deduplicate_csv` | Manual maintenance using standardized URL column |

## 🔒 Security & Privacy
This server runs **locally** on your PC. Your CSV data never leaves your environment; only the specific results of your queries (filtered rows or stats) are sent to the LLM.
