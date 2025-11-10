import streamlit as st
import pandas as pd
import os
from datetime import datetime

# Function to detect columns heuristically
def detect_columns(df):
    date_col = None
    desc_col = None
    amount_col = None
    debit_col = None
    credit_col = None

    # Detect date: column where >80% values parse as date
    for col in df.columns:
        parsed = pd.to_datetime(df[col], errors='coerce')
        if parsed.notna().mean() > 0.8:
            date_col = col
            break

    # Detect amount: look for numeric columns with keywords
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        lower_col = col.lower()
        if 'debit' in lower_col:
            debit_col = col
        elif 'credit' in lower_col:
            credit_col = col
        elif 'amount' in lower_col or 'transaction' in lower_col:
            amount_col = col

    # Detect description: string column with longest avg length
    string_cols = df.select_dtypes(include=['object']).columns
    if string_cols.size > 0:
        avg_lengths = {col: df[col].astype(str).str.len().mean() for col in string_cols}
        desc_col = max(avg_lengths, key=avg_lengths.get, default=None)

    return date_col, desc_col, amount_col, debit_col, credit_col

# Main app
st.title("Transaction Uploader App (YNAB-like)")

uploaded_file = st.file_uploader("Upload your transaction CSV file", type=["csv"])

if uploaded_file is not None:
    # Read CSV (handle no header or varying dialects)
    try:
        df = pd.read_csv(uploaded_file)
    except:
        # Fallback if parsing fails (e.g., no header)
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file, header=None)
        df.columns = [f"Col_{i}" for i in range(df.shape[1])]

    st.subheader("Preview of Uploaded Data")
    st.dataframe(df.head(10))

    # Auto-detect columns
    auto_date, auto_desc, auto_amount, auto_debit, auto_credit = detect_columns(df)

    # User override selectboxes (pre-filled with auto-detections)
    col_options = ['None'] + df.columns.tolist()
    date_col = st.selectbox("Select Date column", col_options, index=col_options.index(auto_date) if auto_date else 0)
    desc_col = st.selectbox("Select Description column", col_options, index=col_options.index(auto_desc) if auto_desc else 0)
    amount_col = st.selectbox("Select Amount column (if single)", col_options, index=col_options.index(auto_amount) if auto_amount else 0)
    debit_col = st.selectbox("Select Debit column (if separate)", col_options, index=col_options.index(auto_debit) if auto_debit else 0)
    credit_col = st.selectbox("Select Credit column (if separate)", col_options, index=col_options.index(auto_credit) if auto_credit else 0)

    if st.button("Process and Add to Master Database"):
        if date_col == 'None' or desc_col == 'None':
            st.error("Please select at least Date and Description columns.")
        else:
            # Extract and standardize
            std_df = df[[date_col, desc_col]].copy()
            std_df.rename(columns={date_col: 'Date', desc_col: 'Description'}, inplace=True)
            
            # Handle amount
            if amount_col != 'None':
                std_df['Amount'] = pd.to_numeric(df[amount_col], errors='coerce')
            elif debit_col != 'None' and credit_col != 'None':
                debits = pd.to_numeric(df[debit_col], errors='coerce').fillna(0) * -1
                credits = pd.to_numeric(df[credit_col], errors='coerce').fillna(0)
                std_df['Amount'] = debits + credits
            elif debit_col != 'None':
                std_df['Amount'] = pd.to_numeric(df[debit_col], errors='coerce').fillna(0) * -1
            elif credit_col != 'None':
                std_df['Amount'] = pd.to_numeric(df[credit_col], errors='coerce').fillna(0)
            else:
                st.error("Please select an Amount, Debit, or Credit column.")
                return

            # Clean: Drop NaNs in key columns, parse dates
            std_df.dropna(subset=['Date', 'Amount'], inplace=True)
            std_df['Date'] = pd.to_datetime(std_df['Date'], errors='coerce')
            std_df.dropna(subset=['Date'], inplace=True)  # Drop unparseable dates

            # Load or create master
            master_file = 'master.xlsx'
            if os.path.exists(master_file):
                master = pd.read_excel(master_file)
                master['Date'] = pd.to_datetime(master['Date'], errors='coerce')
                master = pd.concat([master, std_df], ignore_index=True)
                # Optional: Sort by date and remove duplicates
                master.sort_values('Date', inplace=True)
                master.drop_duplicates(subset=['Date', 'Description', 'Amount'], inplace=True)
            else:
                master = std_df

            # Save back to Excel
            master.to_excel(master_file, index=False, engine='openpyxl')

            st.success(f"Successfully added {len(std_df)} transactions to {master_file}!")
            st.subheader("Updated Master Preview")
            st.dataframe(master.tail(10))