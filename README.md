# Financial Transaction Database Manager (YNAB-like)

A modular Python application for importing financial transaction data from CSV files in various formats, standardizing the data, detecting duplicates, storing it in a database, and managing multiple accounts with budgeting features. Includes both CLI and web interfaces for viewing and managing your finances.

## Features

### Import Features
- **Flexible CSV Parsing**: Handles CSV files with varying formats, column names, and structures
- **Intelligent Column Mapping**: Uses fuzzy matching to automatically map CSV columns to standard fields
- **Data Standardization**: Converts dates to ISO format (YYYY-MM-DD) and normalizes amounts
- **Duplicate Detection**: Prevents duplicate imports based on exact matches of date, description, and amount
- **Account Linking**: Automatically links transactions to accounts (banks, credit cards, investments)
- **Auto-Detection**: Detects account types from filenames and CSV headers
- **Transfer Detection**: Automatically detects and marks inter-account transfers
- **Automatic Categorization**: Rules-based engine for assigning categories to transactions
- **Credit Card Sign Handling**: Automatically handles different credit card CSV formats (e.g., Robinhood vs Chase)
- **Robust Error Handling**: Handles malformed data, missing fields, and invalid values gracefully
- **Chunked Processing**: Efficiently processes large CSV files in chunks
- **Comprehensive Logging**: Tracks import progress, errors, and statistics

### Account Management Features
- **Multiple Account Types**: Support for banks, credit cards, investments, cash, and other account types
- **Account CRUD Operations**: Create, read, update, and delete accounts
- **Balance Calculation**: Automatic balance calculation from transactions
- **Account Filtering**: View transactions filtered by account

### Budgeting Features (YNAB-like)
- **Interactive Budget Dashboard**: Web-based UI with opt-in category budgets and real-time updates
- **Opt-in Category Budgets**: Start empty and add categories via a searchable “+ Add Budget Category” workflow
- **Monthly Budget View**: Track assigned, activity, available, and usage percentage for the selected month
- **Editable Assignments**: Update assigned amounts inline with validation and instant feedback
- **Budget Envelopes**: Allocate funds to categories with budget periods
- **Spending Tracking**: Track spending against allocated budgets with real-time updates pulled directly from transactions
- **Budget Status**: View remaining budget, percentage used, and over/under-budget alerts
- **Color-coded Indicators**: Green for categories with budget remaining, red for overspending
- **Period-based Budgets**: Set budgets for specific time periods (monthly, quarterly, etc.)
- **YNAB Principles**: Built-in tips following the 4 YNAB rules

#### Using the Budget Dashboard

1. Open the **Budget** tab in the Streamlit analytics app and choose the month you want to budget.
2. Click **+ Add Budget Category** to open a searchable dropdown of categories detected from your transactions (transfers are excluded).  
   - If you have no historical data yet, you can define a fallback list via `budget_categories` in `config.yaml`.
3. Enter the **Assigned Amount** (non-negative) and save to create or update the budget envelope for that month.
4. Review the dynamic budget list:
   - **Assigned** values are editable inline—adjust the number input and press **Save** to persist changes.
   - **Activity (Spent)** updates in real time based on transactions for the selected month.
   - **Available** is color-coded (green > 0, yellow = 0, red < 0) for quick status checks.
   - **Budget Used %** highlights spending progress (warning above 90%, red when overspent).
   - Only categories with **Assigned > $0** appear in the list to maintain an opt-in workflow; zero-assigned categories remain hidden until funded.
   - The **Financial Health Snapshot** at the top surfaces income, assignments, unassigned dollars, live spending totals, availability, utilization, and a projection—use the inline income override to keep planning aligned with reality.
   - Combine granular transaction categories with broader budget envelopes via `budget_category_aliases` in `config.yaml` (e.g., map `"Purchase Amazon"` and `"Purchase Target"` into a single `"Shopping"` budget).
5. Expand the **View Budget Table** panel to export a tabular view for analysis or CSV export via Streamlit.

Budgets are saved per month using the first and last day of the selected month. The design is extensible, enabling additional period types in future updates.

> **Tip:** The **Financial Health Snapshot** stays visible at the top, even when no categories are funded yet, so you always have an at-a-glance view of totals.
### Viewing Features
- **Command-Line Viewer**: Query and filter transactions from the terminal
- **Web UI**: Interactive Streamlit-based web interface for viewing transactions
- **Advanced Filtering**: Filter by date range, amount range, description keywords, category, account, and source file
- **Summary Statistics**: View totals, averages, and breakdowns of credits/debits
- **Export Functionality**: Export filtered results to CSV
- **Secure Queries**: Uses parameterized queries to prevent SQL injection

### Analytics & Reporting Features
- **Income/Expense Analysis**: Comprehensive summaries of income, expenses, and net change
- **YNAB-Style Budgeting**: Interactive monthly budgeting with assigned amounts, activity tracking, and available balances
- **Category Breakdown**: Detailed spending analysis by category with percentages
- **Time-Based Trends**: Monthly trend analysis with income, expense, and net tracking, including period-over-period comparisons (previous month/year)
- **Account Comparison**: Compare spending across different accounts
- **Period Comparison**: Compare financial metrics across multiple time periods (1m, 3m, 6m, 12m)
- **Interactive Dashboard**: Streamlit-based analytics dashboard with charts and filters
- **Visualizations**: Pie charts for categories, line/bar charts for trends, comparison charts
- **Flexible Time Frames**: Analyze data for custom date ranges or relative periods
- **Export Reports**: Export reports to text files, CSV, and chart images (PNG)
- **Top Transactions**: View top expenses and income transactions
- **Savings Rate**: Calculate and track savings rate over time

### Transfer Detection & Handling
- **Automatic Detection**: Detects internal transfers during import using configurable regex patterns
- **Pattern Matching**: Recognizes credit card payments, bank transfers, investment transfers, and more
- **Spending Exclusion**: Transfers are automatically excluded from spending analytics to prevent double-counting
- **UI Toggle**: "Include Transfers" checkbox in dashboard to optionally show transfers
- **Batch Reclassification**: Command to retroactively detect transfers in existing transactions
- **Transfer Statistics**: View counts and totals of detected transfers
- **Manual Override**: Ability to manually reclassify transactions as transfers or expenses
- **Configurable Patterns**: Customize transfer detection patterns in `config.yaml`

## Requirements

- Python 3.10 or higher
- See `requirements.txt` for dependencies

## Installation

1. Clone or download this repository

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Ensure `config.yaml` is present in the project directory (it should be included)

## Configuration

The application uses `config.yaml` for configuration. Key settings include:

- **Database**: SQLite database path and connection settings
- **Column Mappings**: Maps common CSV column name variations to standard fields
- **Duplicate Detection**: Configures which fields are used for duplicate detection
- **Processing**: Chunk size, date formats, and decimal precision settings
- **Logging**: Log level and output file settings
- **Budget Categories** *(optional)*: Define a `budget_categories` list to seed the budget dropdown when no transaction history is available

See `config.yaml` for detailed configuration options and examples.

### Data Directory

- All persisted financial data lives inside the auto-created `data/` directory.
- The SQLite file defaults to `data/transactions.db`; override via the `DB_CONNECTION_STRING` env var or `database.connection_string`.
- CSV imports can be stored in `data/` (or sub-folders) so sensitive files stay gitignored by default.
- The CLI and Streamlit apps create the directory if it does not exist, ensuring first-run setups succeed without manual steps.

## Usage

The application supports two main commands: `import` (or `imp`) and `view`.

### Importing Transactions

#### Basic Usage

Import a single CSV file (account auto-detected):
```bash
python main.py import --file path/to/transactions.csv
```

Import with explicit account:
```bash
python main.py import --file transactions.csv --account "Chase Checking"
```

Import with account type:
```bash
python main.py import --file credit_card.csv --account "Visa Card" --account-type credit
```

Import multiple CSV files:
```bash
python main.py import --file transactions1.csv --file transactions2.csv --file transactions3.csv
```

Disable automatic categorization:
```bash
python main.py import --file transactions.csv --no-categorize
```

#### Credit Card CSV Format Handling

Different credit card providers use different sign conventions in their CSV exports:

- **Chase, Capital One, etc.**: Purchases = negative, Payments = positive (matches app's internal format)
- **Robinhood Gold Card**: Purchases = positive, Payments = negative (needs sign inversion)

The app automatically handles these differences:

- **Automatic Sign Inversion**: For accounts like "Robinhood Gold Card", the app automatically inverts transaction signs during import to match the internal accounting convention:
  - Purchases become negative (increase debt)
  - Payments become positive (reduce debt)
  - Refunds become positive (reduce debt)

- **Other Credit Cards**: Cards like Chase already use the correct format and are imported as-is.

**Note**: If you have existing Robinhood transactions with incorrect signs (payments/refunds showing as negative), see the [Troubleshooting](#fixing-credit-card-transaction-signs) section for a fix script.

#### Custom Configuration

Use a custom configuration file:
```bash
python main.py --config custom_config.yaml import --file transactions.csv
```

### Managing Accounts

#### Create an Account
```bash
python main.py account create --name "Chase Checking" --type bank --balance 1000.00
```

#### List All Accounts (Refined Summary)

The account list command provides a refined summary that properly categorizes accounts as **Assets** (positive balances) and **Liabilities** (negative balances):

```bash
python main.py account list
```

**Output Features:**
- **Assets Section**: Bank accounts, savings, investments (sorted by balance descending)
- **Liabilities Section**: Credit cards shown as negative balances/debts (sorted ascending - smallest debts first)
- **Totals**: Shows total assets, total liabilities, and **Net Worth**
- **Proper Accounting**: Credit account balances are automatically inverted to negative values (liabilities)

**Example Output:**
```
===============================================================================================
ASSETS
===============================================================================================
ID    Name                                Type                              Balance
-----------------------------------------------------------------------------------------------
6     Wealthfront Automated Investment    investment           $          53,517.49
5     Wealthfront Cash Savings            savings              $           6,110.01
3     Huntington Bank Checking            bank                 $           1,298.85
-----------------------------------------------------------------------------------------------
TOTAL ASSETS                                                 $          60,926.35
===============================================================================================

===============================================================================================
LIABILITIES
===============================================================================================
ID    Name                                Type                              Balance
-----------------------------------------------------------------------------------------------
4     Robinhood Gold Card                 credit               $            -764.98
1     Chase Freedom Unlimited             credit               $            -293.51
2     Chase Freedom Flex                  credit               $              -0.00
-----------------------------------------------------------------------------------------------
TOTAL LIABILITIES                                            $          -1,058.49
===============================================================================================

===============================================================================================
NET WORTH                                                    $          59,867.86
===============================================================================================
```

**Key Features:**
- **Accurate Financial Picture**: Credit cards properly shown as liabilities (negative)
- **Net Worth Calculation**: Assets - Liabilities = Net Worth
- **Balance Overrides Respected**: Uses override-aware balance calculations
- **Smart Sorting**: Assets by value (highest first), liabilities by amount (smallest debt first)

#### Show Account Details
```bash
python main.py account show --id 1
# or
python main.py account show --name "Chase Checking"
```

#### Update an Account
```bash
python main.py account update --id 1 --name "Updated Name" --balance 1500.00
```

#### Recalculate Account Balance
```bash
python main.py account recalculate --id 1
```

#### Delete an Account
```bash
python main.py account delete --id 1
```

### Managing Budgets

#### Create a Budget
```bash
python main.py budget create --category "Groceries" --amount 500.00 --start 2024-01-01 --end 2024-01-31
```

#### List All Budgets
```bash
python main.py budget list
```

#### View Budget Status
```bash
# View all budget statuses
python main.py budget status

# View specific category
python main.py budget status --category "Groceries"
```

#### Update a Budget
```bash
python main.py budget update --id 1 --amount 600.00
```

#### Delete a Budget
```bash
python main.py budget delete --id 1
```

### Wealthfront Cash Savings & Investment Tracking

The app provides special support for Wealthfront Cash Savings accounts with automatic transfer detection and investment balance tracking.

#### Import Wealthfront Cash Savings

```bash
# Import with automatic transfer detection and investment balance prompt
python main.py import --file wealthfront_cash.csv --wealthfront

# Import without prompting for investment update
python main.py import --file wealthfront_cash.csv --account "Wealthfront Cash Savings" --account-type savings
```

When using the `--wealthfront` flag:
1. Automatically creates/uses "Wealthfront Cash Savings" account (savings type)
2. Detects transfers to investment account (e.g., "Transfer to Automated Investing")
3. Prompts you to manually update your investment balance
4. Creates "Wealthfront Automated Investment" account if needed

#### Manual Balance Updates

For investment and savings accounts where balance cannot be automatically calculated from transactions:

```bash
# Update investment balance
python main.py update-balance --account "Wealthfront Automated Investment" --balance 12345.67

# Update with notes
python main.py update-balance --account "Wealthfront Automated Investment" --balance 12500.00 --notes "Q4 2024 balance"

# View balance history
python main.py update-balance --account "Wealthfront Automated Investment" --history

# Show more history entries
python main.py update-balance --account "Wealthfront Automated Investment" --history --limit 20
```

Balance updates are tracked with timestamps, allowing you to see how your investment balance changes over time.

#### Configure Transfer Patterns

Edit `config.yaml` to customize transfer detection patterns:

```yaml
wealthfront:
  transfer_patterns:
    - "Transfer to Automated Investing"
    - "Transfer to Investment"
    - "Wealthfront Investment"
    - "Auto-Invest"
    - "Transfer.*Investing"
  
  cash_account_name: "Wealthfront Cash Savings"
  investment_account_name: "Wealthfront Automated Investment"
  prompt_investment_update: true
```

### Balance Overrides (Handling Incomplete Historical Data)

When you have incomplete transaction history but know your account balance as of a certain date, you can set a **balance override**. The app will then calculate your current balance as: `override_balance + sum(transactions after override_date)`.

This is essential for maintaining accurate balances when:
- You only have partial transaction history
- Starting fresh with accounts that have existing balances
- Reconciling with bank statements after missing data

#### Set a Balance Override

```bash
# Set override for an account
python main.py balance-override set --account "Wealthfront Cash Savings" --date 2024-01-01 --balance 5000.00 --notes "Balance from bank statement"

# Set override without notes
python main.py balance-override set --account "Chase Freedom Unlimited" --date 2024-06-01 --balance 1250.75
```

#### List Balance Overrides

```bash
# View all overrides for an account
python main.py balance-override list --account "Wealthfront Cash Savings"
```

Output shows:
- Override ID
- Override date
- Override balance
- When it was created
- Notes
- Current calculated balance (override + subsequent transactions)

#### Compare Balances

```bash
# See balance with and without overrides
python main.py balance-override compare --account "Wealthfront Cash Savings"
```

Shows:
- Stored balance (in database)
- Calculated balance (with overrides)
- Difference
- Latest override details

#### Delete a Balance Override

```bash
# Delete an override by ID (get ID from list command)
python main.py balance-override delete --id 1
```

#### How Balance Calculation Works

1. **With Override**: 
   - Finds the most recent override where `override_date <= query_date`
   - Sums transactions where `transaction.date > override_date AND transaction.date <= query_date`
   - Returns `override_balance + transaction_sum`

2. **Without Override**:
   - Sums all transactions where `transaction.date <= query_date`
   - Returns transaction sum

3. **Multiple Overrides**:
   - You can set multiple overrides for different dates
   - The app automatically uses the most recent one for calculations
   - Useful for periodic reconciliations

#### Example Workflow

```bash
# 1. Import transactions starting from June 2024
python main.py import --file transactions.csv --account "My Account"

# 2. You know the balance was $5,000 on June 1, 2024
python main.py balance-override set --account "My Account" --date 2024-06-01 --balance 5000.00 --notes "Opening balance from statement"

# 3. View current balance (override + transactions after June 1)
python main.py account list

# 4. Check the calculation
python main.py balance-override compare --account "My Account"
```

### Transfer Detection & Reclassification

The app automatically detects internal transfers (like credit card payments, bank-to-savings transfers, investment contributions) and excludes them from spending analytics to prevent double-counting.

#### How It Works

1. **During Import**: Transactions are automatically scanned against configurable patterns
2. **Pattern Matching**: Uses regex to identify transfer descriptions (e.g., "Credit Crd-Pay", "Transfer to Robinhood")
3. **Marking**: Detected transfers are flagged with `is_transfer=1` in the database
4. **Analytics**: Transfers are automatically excluded from spending totals and category breakdowns
5. **UI Control**: Use the "Include Transfers" checkbox in the dashboard to optionally show them

#### Common Transfer Patterns Detected

The following patterns are automatically detected (configured in `config.yaml`):

- **Credit Card Payments**: "Credit Crd-Pay", "EDI PMYTS", "DEBIT PMTS", "Payment to Credit"
- **General Transfers**: "Transfer to", "Transfer from", "Internal Transfer", "ACH Transfer"
- **Investment Transfers**: "Payment to Robinhood", "Transfer to Wealthfront", "Payment to Investment"
- **Savings/Checking**: "Transfer to Savings", "Transfer to Checking", "Payment to Savings"
- **Online Banking**: "Online Banking Transfer", "ZELLE TRANSFER", "VENMO TRANSFER TO BANK"

#### Reclassify Existing Transactions

If you've already imported transactions before enabling transfer detection, or if you've updated your patterns, you can retroactively detect transfers:

```bash
# Dry run - see what would be detected without making changes
python main.py reclassify-transfers --dry-run

# Actually reclassify transactions
python main.py reclassify-transfers

# Show statistics before and after
python main.py reclassify-transfers --stats
```

Example output:
```
======================================================================
Transfer Reclassification
======================================================================

Scanning transactions for transfers...

Results:
  Total Transactions Scanned: 523
  Transfers Detected: 42
  Transactions Updated: 42

✓ Reclassification complete!

Note: Transfers are excluded from spending analytics by default.
      Use the 'Include Transfers' checkbox in the UI to include them.
======================================================================
```

#### Customizing Transfer Patterns

Edit `config.yaml` to add your own transfer patterns:

```yaml
transfer_detection:
  enabled: true
  patterns:
    - "Credit Crd-Pay"
    - "Your Bank Name Transfer"
    - "Custom Pattern.*Here"
  transfer_category: "Transfer"  # Category to assign
  log_detected_transfers: true   # Log detected transfers
```

#### Manual Reclassification

If the automatic detection misclassifies a transaction, you can manually correct it:

1. **In the UI**: Navigate to the transaction in a category drill-down view
2. **Future Enhancement**: Manual override buttons (coming soon)
3. **Database**: Update the `is_transfer` field directly if needed

#### Why This Matters

Without transfer detection, your spending analytics would include:
- Credit card payment of $1,000 from checking → Counted as $1,000 expense ❌
- But the actual spending is already tracked in credit card transactions
- **Result**: Your spending appears doubled!

With transfer detection:
- Credit card payment is marked as transfer → Excluded from spending totals ✅
- Only actual purchases count toward spending
- **Result**: Accurate spending analysis!

#### Built-in Safeguards

To prevent double-counting from slipping through, the app includes **multiple safeguards**:

1. **Pattern-Based Detection**: 63+ regex patterns in `config.yaml` catch most common transfer formats
2. **Credit Card Payment Safeguard**: Automatically detects payments from credit card accounts even with generic descriptions like "Payment"
3. **Import-Time Detection**: Transfers are flagged during CSV import (real-time)
4. **Batch Reclassification**: Run `python main.py reclassify-transfers` to retroactively detect transfers in existing data
5. **Account-Type Awareness**: Checks account type (credit/bank/investment) to identify context-specific transfers

**Example**: If your credit card CSV has a transaction with description "Payment" (no other details), the safeguard will:
- Check that it's from a `credit` type account
- Detect "payment" keyword in description
- Automatically mark it as `is_transfer=1`
- Exclude it from spending analytics

This prevents frustrating double-counting that would otherwise go unnoticed!

### Viewing Transactions

#### Command-Line Viewer

View all transactions:
```bash
python main.py view
```

Filter by date range:
```bash
python main.py view --date-start 2024-01-01 --date-end 2024-12-31
```

Filter by amount range:
```bash
python main.py view --amount-min -100 --amount-max 1000
```

Search by description:
```bash
python main.py view --description "AMAZON"
```

Filter by category:
```bash
python main.py view --category "Shopping"
```

Combine multiple filters:
```bash
python main.py view --date-start 2024-01-01 --category "Food & Drink" --amount-max 0
```

Limit results and sort:
```bash
python main.py view --limit 50 --sort-by amount --sort-asc
```

Show summary statistics:
```bash
python main.py view --stats
```

Export filtered results to CSV:
```bash
python main.py view --date-start 2024-01-01 --export transactions_2024.csv
```

#### Web UI Viewer

Launch the interactive Streamlit web interface:
```bash
python main.py view --ui
```

This will open a web browser with an interactive interface where you can:
- Apply filters using sidebar controls
- View transactions in an interactive table
- See summary statistics and breakdowns
- Export results to CSV
- Sort and paginate through results

The web UI provides a more user-friendly experience for exploring your transaction data.

#### View Command Options

- `--date-start YYYY-MM-DD`: Filter transactions from this date (inclusive)
- `--date-end YYYY-MM-DD`: Filter transactions up to this date (inclusive)
- `--amount-min FLOAT`: Minimum transaction amount (inclusive)
- `--amount-max FLOAT`: Maximum transaction amount (inclusive)
- `--description TEXT`: Search keywords in description (case-insensitive)
- `--category TEXT`: Filter by category (case-insensitive partial match)
- `--source-file TEXT`: Filter by source file name (case-insensitive partial match)
- `--account-id N`: Filter by account ID
- `--account-name TEXT`: Filter by account name (case-insensitive partial match)
- `--limit N`: Maximum number of transactions to display
- `--offset N`: Number of transactions to skip (for pagination)
- `--sort-by COLUMN`: Column to sort by (id, date, description, amount, category, source_file)
- `--sort-asc`: Sort ascending (default: descending)
- `--stats`: Show summary statistics
- `--export FILE`: Export results to CSV file
- `--ui`: Launch Streamlit web UI instead of CLI

### Analytics & Reports

The analyze command provides comprehensive financial analytics with visualizations and detailed reports.

#### Generate Full Report (CLI)

Generate a comprehensive report with all analytics:
```bash
python main.py analyze
```

Or specify a time frame:
```bash
python main.py analyze --time-frame 6m
```

#### Specific Report Types

Generate income/expense summary:
```bash
python main.py analyze --report-type summary --time-frame 3m
```

Generate category breakdown:
```bash
python main.py analyze --report-type categories --time-frame 6m
```

Generate monthly trends:
```bash
python main.py analyze --report-type trends --time-frame 12m
```

Generate account summary:
```bash
python main.py analyze --report-type accounts --time-frame all
```

Compare multiple periods:
```bash
python main.py analyze --report-type comparison --periods "1m,3m,6m,12m"
```

#### Export Reports

Export category report to CSV:
```bash
python main.py analyze --report-type categories --time-frame 6m --export-csv categories.csv
```

Export category chart to image:
```bash
python main.py analyze --report-type categories --time-frame 6m --export-chart categories.png
```

Export full report with all artifacts:
```bash
python main.py analyze --report-type full --time-frame 12m --output-dir reports/
```

#### Interactive Analytics Dashboard

Launch the Streamlit analytics dashboard:
```bash
python main.py analyze --ui
```

The dashboard provides:
- **Overview Tab**: Comprehensive financial snapshot with:
  - **Spending by Category**: Interactive pie chart with total spending and category count metrics
  - **Income by Category**: Interactive pie chart with total income and category count metrics
  - **Enhanced Account Balances**: Modern, interactive account section featuring:
    - Large color-coded KPI cards for Total Assets, Total Liabilities, and Net Worth
    - Interactive pie charts showing asset and liability distributions with hover tooltips
    - Net worth progress bar with configurable goal tracking
    - Collapsible account cards with icons, balance history sparklines, and trend indicators
    - Time frame filter (Current, Last Month, Last Quarter, Custom Date) for historical snapshots
    - Net worth trend chart showing 90-day historical data
    - CSV export functionality for account summaries
    - Balance calculations respect override-aware logic for accurate historical views
  - **Monthly Trends**: 6-month trend chart showing income, expenses, and net change with optional period-over-period comparison (Previous Month or Previous Year)
  - **Top Expenses**: Table of your largest expense transactions
  - All metrics respect the "Include Transfers" filter for accurate analysis
- **Budget Tab**: YNAB-style monthly budgeting with editable assignments, activity tracking, and overspending alerts
- **Spending Categories Tab**: Detailed spending category breakdown with interactive pie chart and drill-down to individual transactions
- **Income Categories Tab**: Detailed income category breakdown with interactive pie chart and drill-down to individual transactions
- **Trends Tab**: Monthly income/expense trends with interactive line/bar chart and period-over-period comparison functionality
- **Accounts Tab**: Account balances with assets/liabilities split, drill-down details, and transaction history:
  - **Assets Section**: Shows all asset accounts (bank, savings, investment) with balances
  - **Liabilities Section**: Shows all liability accounts (credit cards) with balances owed
  - **Net Worth Metrics**: Total Assets, Total Liabilities, and Net Worth prominently displayed
  - **Drill-Down Details**: Expand any account to see:
    - Balance calculation breakdown (with balance overrides if set)
    - Recent transactions since override date (or all transactions if no override)
    - Running balance column showing how balance changes with each transaction
    - Transaction summary (income, expenses, net change)
  - **Visual Charts**: Bar charts for assets and liabilities
  - **Export**: Download account balances as CSV
- **Comparison Tab**: Period comparison with grouped bar chart

#### Using the Enhanced Account Balances Section

The Overview tab includes a modernized Account Balances section with powerful features for tracking your financial position:

**Key Features:**

1. **Large KPI Cards**: View Total Assets, Total Liabilities, and Net Worth at a glance with color-coded metrics
   - Green for positive values (assets, positive net worth)
   - Red for negative values (liabilities, negative net worth)
   - Hover for additional context and help text

2. **Net Worth Goal Tracking**:
   - Set your target net worth in the sidebar under "🎯 Net Worth Goal"
   - Default goal is $100,000 (configurable)
   - Progress bar shows completion percentage with encouraging messages
   - Goal persists in `config.yaml` for future sessions

3. **Interactive Pie Charts**:
   - Separate charts for asset and liability distributions with distinct, contrasting colors
   - Each account gets a unique color that matches its card border
   - Dark borders between pie slices for better visual separation
   - Interactive legend on the right side showing all accounts with color coding
   - Hover over slices to see account name, type, balance, and percentage
   - Assets use green/blue/purple spectrum, liabilities use red/orange/pink spectrum

4. **Account Cards** (Expanded by Default):
   - Lists are automatically expanded for quick access to all accounts
   - Each card features:
     - Colored left border matching the pie chart slice
     - Account type icon (🏦 bank, 💳 credit, 📈 investment, etc.)
     - Account name and type
     - Current balance (color-coded green for positive, red for negative)
     - 30-day balance history sparkline (hover for details)
     - Subtle background highlighting
   - Accounts sorted by balance (assets high-to-low, liabilities low-to-high)
   - Color-coded borders make it easy to identify which account corresponds to which pie slice

5. **Time Frame Filter**:
   - View account balances as of any date:
     - **Current**: Today's balances (default)
     - **Last Month**: End of last month
     - **Last Quarter**: 90 days ago
     - **Custom Date**: Click "📅 Custom Date" to select any specific date
   - Historical balances use override-aware calculations for accuracy

6. **Net Worth Trend Chart**:
   - 90-day area chart showing net worth progression
   - Color indicates trend direction (green up, red down)
   - Hover for daily net worth values

7. **Export Functionality**:
   - Click "📥 Export Account Summary CSV" to download account data
   - Includes all accounts with IDs, names, types, and balances
   - Filename includes date for easy organization

**Setting Your Net Worth Goal:**

1. Launch the dashboard: `python main.py analyze --ui`
2. In the sidebar, scroll to "🎯 Net Worth Goal"
3. Enter your target net worth (e.g., $150,000)
4. Click "💾 Save Goal" to persist the value
5. The Overview tab will now show your progress towards this goal

**Configuration:**

You can also edit `config.yaml` to set default preferences:

```yaml
# Net worth goal (default: $100,000)
net_worth_goal: 150000.0

# Show sparklines in account cards (default: true)
show_sparklines: true

# Number of days of history for account sparklines (default: 30)
balance_history_days: 30

# Number of days for net worth trend chart (default: 90)
net_worth_history_days: 90
```

**Tips:**
- Set a realistic goal based on your current net worth and timeline
- Use the time frame filter to track historical progress
- Account cards are expanded by default for quick access to all details
- Look for the colored borders - they match the pie chart colors to help you identify accounts quickly
- The pie chart legend on the right shows all accounts with their corresponding colors
- Export data regularly for offline analysis or record-keeping
- Sparklines help identify accounts with unusual activity
- The UI is optimized for viewing at 75% zoom for a better overview of all data

#### Using the Budget Dashboard

The Budget tab in the UI provides a complete YNAB-style budgeting experience:

**Features:**
1. **Monthly View**: Select any month to view or edit budgets
2. **Category Table**: Editable table showing:
   - **Category**: Transaction category name
   - **Assigned**: Your budget amount (editable)
   - **Activity (Spent)**: Actual spending in this category
   - **Available**: Remaining budget (Assigned - Activity)
   
3. **Budget Metrics**: At-a-glance summary showing:
   - Total Assigned
   - Total Spent
   - Total Available
   - Budget Used percentage

4. **Budget Status**:
   - ⚠️ Over Budget categories highlighted in red
   - ✅ Categories with remaining budget shown in green
   - Expandable sections for details

5. **YNAB Principles**:
   - Built-in tips following the 4 YNAB rules
   - Guidance on assigning every dollar
   - Advice on rolling with the punches

**To Use:**
1. Launch the dashboard: `python main.py analyze --ui`
2. Click on the **"Budget"** tab in the sidebar
3. Select the month you want to budget for
4. Edit the "Assigned" column to set your budgets
5. Click "💾 Save Budget Changes" to save
6. Monitor your spending throughout the month!

**Tips:**
- Start with the current month and work backwards if needed
- Use the "Budget Tips" expander for YNAB guidance
- Check the over/under budget sections regularly
- Adjust budgets as needed - flexibility is key!

#### Category Drill-Down Feature

Both the Spending Categories and Income Categories tabs include interactive drill-down functionality:

**How to Use:**
1. Go to the **"Spending Categories"** or **"Income Categories"** tab in the dashboard
2. Click on any category button (e.g., "📂 Groceries" for spending or "💰 Salary" for income) in the detailed breakdown
3. View all transactions in that category for your selected time frame
4. Transactions are sorted by date (most recent first)
5. Click **"⬅️ Back to Categories"** or **"⬅️ Back to Income Categories"** to return to the overview

**Drill-Down View Shows:**
- **Total Spent/Received**: Sum of all transactions in the category
- **Average Transaction**: Mean transaction amount
- **Transaction Count**: Number of transactions
- **Transaction Table**: Complete list with date, description, amount, account, and source
- **Export Option**: Download transactions for that category as CSV

**Benefits:**
- Quickly identify specific spending or income patterns
- Review individual transactions in any category
- Verify categorization accuracy
- Export category-specific data for detailed analysis

All charts are interactive (hover for details, click to filter) and data can be exported to CSV.

#### Period-over-Period Comparison Feature

The Monthly Trends chart now supports period-over-period comparisons, allowing you to overlay data from previous periods for better insight into financial trends.

**How to Use:**
1. Navigate to the **Overview** tab or **Trends** tab in the dashboard
2. In the "Monthly Trends" section, use the "Compare with:" dropdown
3. Select one of the following options:
   - **None**: Show only current period data (default)
   - **Previous Month**: Overlay data from the previous month for each period shown
   - **Previous Year**: Overlay data from the same month in the previous year

**Features:**
- **Layered Visualization**: Current period shown with solid bars/lines, comparison period shown with semi-transparent/dashed overlays
- **Interactive Tooltips**: Hover over chart elements to see values for both current and comparison periods
- **Percentage Change Summary**: Automatically displays percentage changes for income, expenses, and net with color-coded indicators:
  - Green for positive changes (income/net increases, expense decreases)
  - Red for negative changes (income/net decreases, expense increases)
- **Smart Alignment**: Comparison periods are automatically aligned with current periods (e.g., if viewing Jan-Oct 2025, previous year shows Jan-Oct 2024)

**Edge Cases Handled:**
- **Missing Data**: Shows warning message if no comparison data is available
- **Short Time Frames**: Disables "Previous Month" option if less than 2 months of data available
- **Negative Values**: Properly handles negative net values with appropriate color coding (red for losses)

**Example Use Cases:**
- Compare this month's spending to last month to track month-over-month trends
- Compare Q1 2025 to Q1 2024 to see year-over-year growth
- Identify seasonal patterns by comparing same months across years
- Track improvement in savings rate by comparing net income across periods

**Technical Details:**
- Comparison data is fetched using the same account filters as the current period
- Transfers are excluded from both current and comparison data for accurate analysis
- Percentage changes are calculated using the formula: `((current - comparison) / |comparison|) * 100`
- Chart uses Altair's layered visualization capabilities for smooth overlays

#### Time Frame Options

- `1m` - Last 1 month
- `3m` - Last 3 months
- `6m` - Last 6 months (default)
- `12m` - Last 12 months
- `all` - All time
- `YYYY-MM-DD:YYYY-MM-DD` - Custom date range (e.g., `2024-01-01:2024-12-31`)

#### Analyze Command Options

- `--report-type TYPE`: Type of report to generate (summary, categories, trends, accounts, comparison, full)
- `--time-frame FRAME`: Time frame for analysis (e.g., '1m', '3m', '6m', '12m', 'all', or date range)
- `--account-id N`: Filter by specific account ID
- `--account-type TYPE`: Filter by account type (bank, credit, investment, cash, other)
- `--top-n N`: Limit category reports to top N categories
- `--export FILE`: Export text report to file
- `--export-csv FILE`: Export data to CSV file
- `--export-chart FILE`: Export chart to PNG file
- `--output-dir DIR`: Output directory for full report export
- `--periods LIST`: Comma-separated periods for comparison (e.g., '1m,3m,6m')
- `--ui`: Launch Streamlit analytics dashboard instead of CLI

### Example CSV Format

The application can handle various CSV formats. Here are some examples:

**Format 1:**
```csv
Date,Description,Amount,Category
2024-01-15,Grocery Store,-45.50,Groceries
2024-01-16,Gas Station,-30.00,Transportation
```

**Format 2:**
```csv
Transaction Date,Transaction Description,Transaction Amount,Type
01/15/2024,Store Purchase,45.50,Groceries
01/16/2024,Fuel Payment,30.00,Transportation
```

**Format 3:**
```csv
date,description,amount
2024-01-15,Salary Deposit,2500.00
2024-01-16,Utility Bill,-120.50
```

The application will automatically detect and map these columns to the standard schema.

## Architecture

The application is organized into modular components:

### Import Components
- **`data_ingestion.py`**: Reads CSV files and handles format detection
- **`data_standardization.py`**: Maps columns and standardizes data types/formats
- **`duplicate_detection.py`**: Generates unique hashes and detects duplicates
- **`database_ops.py`**: Manages database connections and operations (includes query functions)
- **`main.py`**: Orchestrates the import and view processes

### Viewer Components
- **`data_viewer.py`**: Core functions to query database, apply filters, and format data (returns pandas DataFrames)
- **`cli_viewer.py`**: Command-line interface for viewing transactions
- **`ui_viewer.py`**: Streamlit-based web UI for interactive transaction viewing

### YNAB-like Components
- **`account_management.py`**: CRUD operations for accounts and balance calculations
- **`enhanced_import.py`**: Extended import with account linking and transfer detection
- **`categorization.py`**: Rules-based transaction categorization engine
- **`budgeting.py`**: Budget management (create, track, report)

### Analytics Components
- **`analytics.py`**: Core data aggregation and analysis functions (income/expense summaries, category breakdowns, trends, comparisons)
- **`report_generator.py`**: Format analytics data into reports (text tables, CSV, charts using matplotlib)
- **`cli_analytics.py`**: Command-line interface for generating analytics reports
- **`ui_analytics.py`**: Streamlit-based interactive analytics dashboard with Altair visualizations

## Database Schema

### Transactions Table
Transactions are stored in a `transactions` table with the following structure:

- `id`: Auto-incrementing primary key
- `date`: Transaction date (datetime)
- `description`: Transaction description (string, max 500 chars)
- `amount`: Transaction amount (float, 2 decimal places)
- `category`: Optional transaction category (string, max 100 chars)
- `account`: Optional account name (legacy field, string, max 100 chars)
- `account_id`: Foreign key to accounts table
- `source_file`: Source CSV filename (string)
- `import_timestamp`: When the record was imported (datetime)
- `duplicate_hash`: MD5 hash of key fields for duplicate detection (unique)
- `is_transfer`: Flag indicating if transaction is a transfer (0 or 1)
- `transfer_to_account_id`: Foreign key to destination account for transfers

### Accounts Table
Accounts are stored in an `accounts` table:

- `id`: Auto-incrementing primary key
- `name`: Account name (unique, string, max 100 chars)
- `type`: Account type enum (bank, credit, investment, savings, cash, other)
- `balance`: Current account balance (float)
- `created_at`: Timestamp when account was created
- `updated_at`: Timestamp when account was last updated

### Budgets Table
Budgets are stored in a `budgets` table:

- `id`: Auto-incrementing primary key
- `category`: Category name (string, max 100 chars, indexed)
- `allocated_amount`: Amount allocated to this category (float)
- `period_start`: Start date of budget period (datetime)
- `period_end`: End date of budget period (datetime)
- `created_at`: Timestamp when budget was created
- `updated_at`: Timestamp when budget was last updated

### Balance History Table
Historical balance snapshots are stored in a `balance_history` table:

- `id`: Auto-incrementing primary key
- `account_id`: Foreign key to accounts table (indexed)
- `balance`: Balance at this point in time (float)
- `timestamp`: When this balance was recorded (datetime, indexed)
- `notes`: Optional notes about this balance update (string, max 255 chars)

This table tracks manual balance updates for investment and savings accounts, allowing you to see balance changes over time.

### Balance Overrides Table
Balance overrides for handling incomplete historical data are stored in a `balance_overrides` table:

- `id`: Auto-incrementing primary key
- `account_id`: Foreign key to accounts table (indexed)
- `override_date`: Date for which the balance is known (date, indexed)
- `override_balance`: Known balance as of override_date (float)
- `created_at`: When this override was created (datetime)
- `notes`: Optional notes about this override (string, max 255 chars)

This table allows setting a known balance as of a specific date. Current balance is calculated as: `override_balance + sum(transactions after override_date)`. This is essential when you have incomplete transaction history but know the balance at a certain point in time.

## Duplicate Detection

Duplicates are detected based on an exact match of:
- Date
- Description
- Amount

All three fields must match exactly for a transaction to be considered a duplicate. Transactions with the same amount but different dates or descriptions are **not** considered duplicates.

The duplicate detection uses MD5 hashing of the concatenated key fields for efficient comparison.

## Error Handling

The application handles various error conditions:

- **Malformed CSV files**: Skips invalid rows and logs warnings
- **Missing required fields**: Logs errors and skips affected rows
- **Invalid data types**: Attempts conversion, logs warnings on failure
- **Database errors**: Rolls back transactions and logs errors
- **File I/O errors**: Logs errors and continues with other files

All errors are logged to both console and optionally to a log file (configured in `config.yaml`).

## Testing

Run the test suite:
```bash
pytest tests/test_import.py -v
```

Run tests with coverage:
```bash
pytest tests/test_import.py --cov=. --cov-report=html
```

## Extension Points

The application is designed to be extensible:

### Adding New File Formats

To support additional file formats (e.g., XLSX, JSON), you can:

1. Create a new reader class similar to `CSVReader` in `data_ingestion.py`
2. Implement the same interface (read, validate, get_file_info methods)
3. Update `main.py` to detect file type and use the appropriate reader

### Adding API Integration

To import from APIs (e.g., bank APIs):

1. Create an abstract base class for importers
2. Implement API-specific importer classes
3. Update `main.py` to support API sources in addition to file sources

### Customizing Duplicate Detection

Modify the `key_fields` in `config.yaml` to change which fields are used for duplicate detection. For example, to include category:

```yaml
duplicate_detection:
  key_fields:
    - "date"
    - "description"
    - "amount"
    - "category"
```

### Export Functionality

To export transactions from the database:

1. Add export methods to `database_ops.py`
2. Create a new module `data_export.py` for formatting exports
3. Add export commands to `main.py` or create a separate `export.py` script

### UI Integration

To add a web UI using Streamlit:

1. Create `app.py` using Streamlit
2. Use the existing modules for backend operations
3. Add file upload and import status display components

Example structure:
```python
import streamlit as st
from main import import_transactions, load_config

st.title("Financial Transaction Importer")
uploaded_file = st.file_uploader("Upload CSV", type="csv")
if uploaded_file:
    # Process file...
```

## Troubleshooting

### Common Issues

**Issue**: "Missing required fields in CSV"
- **Solution**: Check that your CSV has columns that match date, description, and amount. The application uses fuzzy matching, so variations like "Transaction Date" should work. Check `config.yaml` for supported column name variations.

**Issue**: "Failed to parse date"
- **Solution**: Ensure dates are in a recognized format. Supported formats are listed in `config.yaml` under `processing.date_formats`. Common formats include YYYY-MM-DD, MM/DD/YYYY, etc.

**Issue**: "Database locked" or SQLite errors
- **Solution**: Ensure no other process is accessing the database file. Close any database viewers or other applications using the database.

**Issue**: All transactions marked as duplicates
- **Solution**: Check that the duplicate detection key fields match your expectations. The default uses date, description, and amount. Ensure these fields are being parsed correctly.

**Issue**: Credit card payments and refunds showing as expenses (wrong sign)
- **Solution**: Some credit card providers (like Robinhood) use inverted signs in their CSV exports. The app automatically handles this for future imports, but if you have existing transactions with wrong signs, use the fix script below.

### Fixing Credit Card Transaction Signs

If you imported credit card transactions before the automatic sign handling was implemented, or if payments/refunds are showing with incorrect signs, you can fix existing transactions:

#### For Robinhood Gold Card

The app includes a fix script specifically for Robinhood Gold Card transactions:

```bash
# Dry run - preview what will be changed (no database changes)
python fix_robinhood_payments.py

# Apply the fix (inverts only payments and refunds, leaves purchases unchanged)
python fix_robinhood_payments.py --apply --force
```

**What it does:**
- Finds all **payments** and **refunds** in the Robinhood Gold Card account
- Inverts their signs (negative → positive) so they correctly reduce debt
- Leaves purchases unchanged (they should remain negative)
- Recalculates the account balance

**Example:**
- Before: Payment = -$982.68 (incorrectly increases debt)
- After: Payment = +$982.68 (correctly reduces debt)

**Note**: This script only affects the Robinhood Gold Card account. Other credit cards (like Chase) already use the correct format and don't need fixing.

## License

This project is provided as-is for educational and personal use.

## Contributing

To extend this application:

1. Follow PEP 8 style guidelines
2. Add type hints to all functions
3. Include docstrings (Google-style)
4. Write unit tests for new functionality
5. Update this README with new features

## Support

For issues or questions, please check:
1. The log file (if configured) for detailed error messages
2. The configuration file for correct settings
3. The test suite to understand expected behavior

