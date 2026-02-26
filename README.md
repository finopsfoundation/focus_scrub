# FOCUS Scrub

A Python command-line tool for scrubbing sensitive data from FOCUS billing files with consistent, reproducible mappings.

## Features

### Data Scrubbing
- **Account ID Scrubbing**: Consistently maps account IDs (numeric, UUIDs, ARNs) across all columns
- **Name Anonymization**: Replaces account names with stellar-themed generated names
- **Date Shifting**: Shifts date/datetime values by a configurable number of days
- **Commitment Discount IDs**: Intelligently scrubs complex IDs containing embedded account numbers and UUIDs

### Mapping Engine
- **Component-Level Mappings**: Centralized mapping engine ensures consistency across all columns
  - `NumberId`: Maps numeric IDs (e.g., 12-digit AWS account IDs)
  - `UUID`: Maps UUIDs to new random UUIDs
  - `Name`: Maps names to stellar-themed names (e.g., "Nebula Alpha")
  - `ProfileCode`: Maps dash-separated codes preserving structure

### Consistency Guarantees
- Same account ID (e.g., `407116685360`) maps to the same value whether it appears:
  - As a standalone `SubAccountId`
  - Embedded in a `CommitmentDiscountId` ARN
  - In any other account-related column
- Export mappings to ensure consistency across multiple processing runs
- Load mappings from previous runs to maintain referential integrity

### File Format Support
- **Input formats**: `.csv`, `.csv.gz`, `.parquet`
- **Output formats**: `csv-gzip`, `parquet`
- Process single files or entire directories
- Preserves directory structure in output

## Architecture

### Project Layout

- `focus_scrub/focus_scrub/cli.py` - CLI entrypoint
- `focus_scrub/focus_scrub/io.py` - File discovery + read/write logic
- `focus_scrub/focus_scrub/scrub.py` - Deterministic column replacement engine
- `focus_scrub/focus_scrub/handlers.py` - Reusable handler registry + dataset-to-column mapping
- `focus_scrub/focus_scrub/mapping/` - Mapping infrastructure
  - `engine.py` - Central MappingEngine for consistent component mappings
  - `collector.py` - MappingCollector for tracking column-level mappings

### Handler Architecture

Handlers delegate to the shared `MappingEngine` to ensure consistency:
- **AccountIdHandler**: Decomposes complex values (ARNs), extracts components (account IDs, UUIDs), and maps each via the engine
- **StellarNameHandler**: Maps account names to stellar-themed names
- **CommitmentDiscountIdHandler**: Delegates to AccountIdHandler with shared engine
- **DateReformatHandler**: Shifts dates by configured number of days

> Any column without a configured handler is passed through unchanged.

## Setup

1. Install Poetry:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Install project dependencies:
```bash
poetry install
```

## Usage

### Basic Usage

Process files without exporting mappings:
```bash
poetry run focus-scrub <input_path> <output_path> --dataset CostAndUsage
```

### Export Mappings

Export mappings for reuse in subsequent runs:
```bash
poetry run focus-scrub input/ output/ \
  --dataset CostAndUsage \
  --export-mappings mappings.json
```

The exported JSON contains:
- `column_mappings`: Per-column old→new value mappings
- `component_mappings`: Component-level mappings (NumberId, UUID, Name, ProfileCode)

### Load Mappings

Reuse mappings from a previous run to ensure consistency:
```bash
poetry run focus-scrub input2/ output2/ \
  --dataset CostAndUsage \
  --load-mappings mappings.json
```

### Date Shifting

Shift all date columns by 30 days:
```bash
poetry run focus-scrub input/ output/ \
  --dataset CostAndUsage \
  --date-shift-days 30
```

### Output Format

Specify output format (default is `parquet`):
```bash
poetry run focus-scrub input/ output/ \
  --dataset CostAndUsage \
  --output-format csv-gzip
```

### Complete Example

```bash
# First run: Process files and export mappings
poetry run focus-scrub datafiles/AWS datafiles_out/AWS \
  --dataset CostAndUsage \
  --output-format parquet \
  --date-shift-days 30 \
  --export-mappings mappings/aws_mappings.json

# Second run: Process more files using same mappings
poetry run focus-scrub datafiles/AWS_batch2 datafiles_out/AWS_batch2 \
  --dataset CostAndUsage \
  --load-mappings mappings/aws_mappings.json
```

## Supported Datasets

- `CostAndUsage` - Standard cost and usage data
- `ContractCommitment` - Contract commitment data

## Configuration

### Adding Handlers

In `handlers.py`:

1. **Register handler factory** in `HANDLER_FACTORIES`:
```python
HANDLER_FACTORIES: dict[str, HandlerFactory] = {
    "DateReformat": _build_date_reformat_handler,
    "AccountId": _build_account_id_handler,
    "StellarName": _build_stellar_name_handler,
    "YourNewHandler": _build_your_new_handler,
}
```

2. **Map columns to handlers** in `DATASET_COLUMN_HANDLER_NAMES`:
```python
"CostAndUsage": {
    "BillingAccountId": "AccountId",
    "BillingAccountName": "StellarName",
    "YourColumn": "YourNewHandler",
}
```

### How Mappings Work

1. **MappingEngine** creates consistent mappings for primitive components:
   - Numeric IDs always map to same random numeric ID
   - UUIDs always map to same random UUID
   - Names always map to same stellar name

2. **Handlers** decompose complex values and use engine for each component:
   - ARN `arn:aws:ec2:us-east-1:407116685360:reserved-instances/uuid` →
   - Account `407116685360` → maps via `engine.map_number_id()`
   - UUID → maps via `engine.map_uuid()`
   - Result: `arn:aws:ec2:us-east-1:716254982240:reserved-instances/new-uuid`

3. **Consistency** is maintained because the engine remembers all mappings:
   - Same input value always produces same output
   - Works across all columns and all files in a run
   - Can be exported and reloaded for future runs

## Example Output

### Original Data
```
BillingAccountId: 961082193871
SubAccountId: 407116685360
BillingAccountName: The Linux Foundation
CommitmentDiscountId: arn:aws:ec2:us-east-1:407116685360:reserved-instances/ed12ad8c-...
```

### Scrubbed Data
```
BillingAccountId: 736035721513
SubAccountId: 716254982240
BillingAccountName: Nebula Iota
CommitmentDiscountId: arn:aws:ec2:us-east-1:716254982240:reserved-instances/c741d6b8-...
```

Note: The account ID `407116685360` consistently maps to `716254982240` in both the `SubAccountId` column and within the `CommitmentDiscountId` ARN.
