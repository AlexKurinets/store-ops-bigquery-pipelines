# Source freshness and field classes

Generated from `contracts/*.yaml` by `storeops docs`. A test fails if this file is out of date.

| Source | System and protocol | Cloud Scheduler | Freshness SLA | Owner |
|---|---|---|---|---|
| audits | Store audit app; REST, API key header, page paging | `30 6 * * *` America/Chicago | previous business date loaded by 07:00 CT | store operations |
| budgets | Finance budget workbooks; Cloud Storage bucket, one .xlsx per fiscal month, reloaded when a new object generation appears | `0 6 * * *` America/Chicago | current month's file loaded by day 3, 06:00 CT | finance |
| distributor | Foodservice distributor; SFTP with a pinned host key, nightly CSV files and a JSON manifest (SHA-256 and row count per file) | `30 7 * * *` America/Chicago | previous business date loaded by 09:00 CT | purchasing |
| pos | Cloud POS; REST, OAuth2 client credentials, cursor paging | `15 5 * * *` America/Chicago | previous business date loaded by 06:00 CT | store systems team |
| timekeeping | Workforce management system; OData v4, basic auth, $filter and $select, nextLink paging | `15 6 * * *` America/Chicago | previous business date loaded by 07:00 CT | HR systems |

## Fields by class

| Table | keep | hash | mask | restrict | drop | Dropped or restricted fields |
|---|---|---|---|---|---|---|
| audit_checklist_items | 8 | 0 | 1 | 0 | 2 | `submitted_by_name` (drop), `submitted_by_email` (drop) |
| budget_store_month | 7 | 0 | 0 | 0 | 0 | none |
| distributor_invoice_lines | 11 | 0 | 0 | 0 | 1 | `driver_name` (drop) |
| distributor_inventory_counts | 5 | 0 | 0 | 0 | 0 | none |
| pos_transaction_lines | 14 | 1 | 0 | 0 | 1 | `card_number` (drop) |
| timekeeping_shifts | 7 | 1 | 0 | 0 | 0 | none |
| timekeeping_daily_sales | 4 | 0 | 0 | 0 | 0 | none |
| timekeeping_employees | 4 | 1 | 0 | 1 | 6 | `FirstName` (drop), `LastName` (drop), `SSN` (drop), `BirthDate` (drop), `HomeAddress` (drop), `PayRate` (restrict), `Notes` (drop) |
