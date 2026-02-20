# LinkedIn Prospecting CSV Manager (MCP Server)

A high-performance, token-efficient Model Context Protocol (MCP) server designed for managing LinkedIn prospecting data in local CSV files. Built with Python and `pandas`.

## 🚀 Why This Project?

Managing large CSV files directly within an LLM (like Claude or ChatGPT) is inefficient and error-prone:
- **Token Drain**: Reading a 1000-line CSV can consume ~30,000 tokens per operation.
- **Data Corruption**: Manual file writing by LLMs often leads to escaping issues or column mismatches.
- **Scalability**: LLMs struggle with O(n) deduplication and full-file rewrites.

**This MCP server reduces token usage by 60x+** by offloading CSV logic to your local machine.

## 🛠️ Features

- **Efficient Appending**: Add new profiles with automatic deduplication based on LinkedIn URLs.
- **Multi-Criteria Filtering**: Query profiles by Score, Location, Company, or Date.
- **Statistical Analysis**: Get instant distributions of scores, locations, and company sizes.
- **Full-Text Search**: Search across Headline, Company, Match Reason, and Roles.
- **Atomic-like Operations**: Powered by `pandas` for reliability and speed.

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

```json
{
  "mcpServers": {
    "linkedin-prospecting-csv": {
      "command": "uv",
      "args": [
        "--directory",
        "C:\\path\\to\\your\\repo\\antigravity-mcp-csv-add-deduplicate",
        "run",
        "linkedin-prospecting-csv"
      ]
    }
  }
}
```

## 🧪 Testing

### Automated Testing
We use `pytest` with real-world data from the `TESTS` directory:
```bash
$env:PYTHONPATH="src"; uv run --with pandas --with mcp --with pytest --with pytest-asyncio python -m pytest TESTS/test_csv_ops.py
```

### Manual Verification
You can manually check the server's functionality using the provided CSV files:
1. **Stats Check**: Load `TESTS/CSV_3_big.csv` through the `get_csv_stats` tool in Claude.
2. **Dedupe Check**: Attempt to append records from `CSV_1_small.csv` to `CSV_3_big.csv` and verify that already existing records are skipped.

## 🛠️ Available Tools

| Tool | Purpose |
| :--- | :--- |
| `append_profiles_to_csv` | Add new profiles + deduplicate |
| `filter_profiles` | Query profiles by criteria |
| `get_csv_stats` | Summary statistics & breakdowns |
| `export_segment` | Save filtered results to new CSV |
| `search_profiles` | Full-text search across columns |
| `deduplicate_csv` | Manual maintenance/cleanup |

## 🔒 Security & Privacy
This server runs **locally** on your PC. Your CSV data never leaves your environment; only the specific results of your queries (filtered rows or stats) are sent to the LLM.
