from database import delete_transaction, get_transactions_edit, get_account_id_by_name, get_return_id_by_name, get_trans_type_id_by_name ,load_return_types, add_product, add_transaction, get_product_id_by_name, load_products, load_trans_types, load_data, get_balances, get_transactions, get_recon_data, add_customer, validate_login, hash_password, get_user_full_name, load_accounts, update_transaction, update_product
import sqlite3
import pandas as pd
import streamlit as st
import base64
import time
from git import Repo, exc
import os
import tempfile
from datetime import datetime
import pytz

# Function to connect to the SQLite database
def create_connection(db_file='stock_control.db'):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    return conn

# Get the GitHub token from secrets
github_token = st.secrets["github"]["token"]

# Set the repository URL to use HTTPS and include the token
repo_url = f'https://{github_token}@github.com/Ignus70/Koelkamerstock.git'

# Path to the database file
db_file_name = 'stock_control.db'
db_path = db_file_name  # Assuming the database is in the same directory as this script

# Function to generate a download link for the database file
def download_database(db_file):
    if os.path.exists(db_file):
        with open(db_file, 'rb') as f:
            data = f.read()
        b64 = base64.b64encode(data).decode()
        href = f'<a href="data:file/sqlite;base64,{b64}" download="{os.path.basename(db_file)}">Download {os.path.basename(db_file)}</a>'
        return href
    else:
        return "Database file not found."

# Function to commit and push changes to GitHub
def push_to_github():
    try:
        # Initialize or open the Repo object for the current directory
        repo = Repo(os.getcwd()) if os.path.exists('.git') else Repo.init(os.getcwd())

        # Configure user details (this can be skipped if already set globally)
        with repo.config_writer() as git_config:
            git_config.set_value('user', 'name', 'Ignus70')
            git_config.set_value('user', 'email', 'systems@ber.co.za')

        # Set the remote URL using the GitHub token for authentication
        if 'origin' not in [remote.name for remote in repo.remotes]:
            origin = repo.create_remote('origin', repo_url)
        else:
            origin = repo.remote(name='origin')
            origin.set_url(repo_url)  # Ensure it's using the token URL

        # Add the database file to the staging area
        if os.path.exists(db_path):
            repo.git.add(db_file_name)  # Add the specific database file
        else:
            st.error("Database file not found. Cannot push to GitHub.")
            return
        
        # Commit the changes
        if repo.is_dirty(untracked_files=True):
            repo.index.commit("Update database with latest changes")
        else:
            st.info("No changes detected to commit.")
            return
        
        # Push the changes to GitHub
        origin.push(refspec='main:main')  # Replace 'main' with the actual branch name if different
        
        st.success("Changes have been pushed to GitHub successfully!")
    
    except exc.GitCommandError as e:
        st.error(f"Failed to push changes to GitHub: {str(e)}")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

# Main Streamlit app
def main():
    st.set_page_config(
        page_title="Bergendal Koelkamer",
        page_icon="🍊",  # Orange emoji
        layout="wide"
    )

    st.title("Koelkamer Stock")

    # Sidebar menu based on login state
    if 'product_entries' not in st.session_state:
        st.session_state.product_entries = [{'product_id': None, 'qty': 1, 'account_id': None, 'return_id': None}]

    # Initialize session state
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if 'user_id' not in st.session_state:
        st.session_state['user_id'] = None
    if 'customer_name' not in st.session_state:
        st.session_state['customer_name'] = ""
    if 'customer_surname' not in st.session_state:
        st.session_state['customer_surname'] = ""
    if 'is_editor' not in st.session_state:
        st.session_state['is_editor'] = False
    if 'view_mode' not in st.session_state:
        st.session_state['view_mode'] = 'Balans'
    if 'edited_rows' not in st.session_state:
        st.session_state['edited_rows'] = {}

    if st.session_state['logged_in']:
        st.sidebar.title(f"Welcome, {st.session_state['customer_name']} {st.session_state['customer_surname']}")
        option = st.sidebar.selectbox('Choose option:', ['Transaksie', 'Report', 'Nuwe Produk', 'Logout'])
    else:
        st.sidebar.title('Menu')
        option = st.sidebar.selectbox('Choose option:', ['Login', 'Sign Up'])

    if option == 'Login':
        st.header('Login')
        login_email = st.text_input('Email')
        login_password = st.text_input('Password', type='password')
        if st.button('Login'):
            if '@ber.co.za' not in login_email:
                st.error('You do not belong to Bergendal.')
            else:
                user_id, customer_name, customer_surname = validate_login(login_email, login_password)
                if user_id:
                    st.session_state['user_id'] = user_id
                    st.session_state['logged_in'] = True
                    st.session_state['customer_name'] = customer_name
                    st.session_state['customer_surname'] = customer_surname
                    st.session_state['is_editor'] = login_email == 'systems@ber.co.za' or login_email == 'data@ber.co.za'
                    st.success('Login successful!')
                    st.rerun()
                else:
                    st.error('Invalid email or password')

    elif option == 'Sign Up':
        st.header('Sign Up')
        signup_name = st.text_input('Name')
        signup_surname = st.text_input('Surname')
        signup_email = st.text_input('Email')
        signup_password = st.text_input('Password', type='password')
        signup_password_confirm = st.text_input('Confirm Password', type='password')
        if st.button('Sign Up'):
            if '@ber.co.za' not in signup_email:
                st.error('You do not belong to Bergendal.')
            elif signup_password != signup_password_confirm:
                st.error('Passwords do not match')
            else:
                hashed_password = hash_password(signup_password)
                user_id = add_customer(signup_name, signup_surname, signup_email, hashed_password)
                if user_id:
                    st.success('You have successfully signed up! Please log in.')
                    st.rerun()
                else:
                    st.error('Error signing up. Please try again.')

    elif option == 'Logout':
        st.session_state['logged_in'] = False
        st.session_state['user_id'] = None
        st.session_state['customer_name'] = ""
        st.session_state['customer_surname'] = ""
        st.session_state['is_editor'] = False
        st.success('Logged out successfully!')
        st.rerun()

    elif 'logged_in' in st.session_state and st.session_state['logged_in']:
        if option == 'Transaksie':
            st.header('Create a Transaction')
            trans_types, product_list = load_data()
            account_list = load_accounts()
            return_types = load_return_types()
            trans_type_id = st.selectbox('Transaction Type', trans_types, format_func=lambda x: x[1])

            local_tz = pytz.timezone('Africa/Johannesburg')
            new_local = datetime.now(local_tz)

            with st.form("transaction_form"):

                # Two columns for Date & Time and PO Number
                col1, col2 = st.columns(2)

                # Date input on the left
                with col1:
                    transaction_date = st.date_input("Transaction Date", value=new_local.date(), key="transaction_date")
                    po_number = None
                    if trans_type_id[1] in ['Uit', 'Return']:
                        po_number = st.text_input('PO Number')

                # PO Number input on the right
                with col2:
                    transaction_time = st.time_input("Transaction Time", value=new_local.time(), key="transaction_time")

                transaction_datetime = pd.Timestamp.combine(transaction_date, transaction_time)

                col3, col4 = st.columns(2)
                with col3:
                    if st.form_submit_button('Add another product'):
                        st.session_state.product_entries.append({'product_id': None, 'qty': 1, 'account_id': None, 'return_id': None})
                        st.rerun()
                with col4:
                    if st.form_submit_button('Remove last product') and len(st.session_state.product_entries) > 1:
                        st.session_state.product_entries.pop()
                        st.rerun()

                # Dynamically manage product selections
                for i, entry in enumerate(st.session_state.product_entries):
                    st.markdown(
                        """
                        <div style="border: 5px solid #ddd; padding: 0.25px; margin-bottom: 2px; border-radius: 5px;">
                        """, unsafe_allow_html=True
                    )
                    cols1 = st.columns([3, 1])
                    with cols1[0]:
                        entry['product_id'] = st.selectbox('Select Product', product_list, format_func=lambda x: x[1], key=f"product_{i}")
                    with cols1[1]:
                        entry['qty'] = st.number_input('Quantity', value=entry['qty'], min_value=0, key=f"qty_{i}")

                    # Second row: account and return type
                    cols2 = st.columns([3, 2])
                    if trans_type_id[1] in ['Uit', 'Return']:  # Check if transaction type is 'Uit' or 'Return'
                        with cols2[0]:
                            account_names = [account[1] for account in account_list]
                            selected_account = st.radio('Select Account', account_names, key=f"account_{i}")
                            entry['account_id'] = [account[0] for account in account_list if account[1] == selected_account][0]
                    else:
                        entry['account_id'] = None

                    if trans_type_id[1] == 'Return':  # Check if transaction type is 'Return'
                        with cols2[1]:
                            return_names = [return_type[1] for return_type in return_types]
                            selected_return = st.radio('Select Return Type', return_names, key=f"return_{i}")
                            entry['return_id'] = [return_type[0] for return_type in return_types if return_type[1] == selected_return][0]
                    else:
                        entry['return_id'] = None

                    st.markdown("</div>", unsafe_allow_html=True)

                # Form submission
                submitted = st.form_submit_button("Submit Transaction")
                if submitted:
                    product_ids = [entry['product_id'][0] for entry in st.session_state.product_entries]
                    quantities = [entry['qty'] for entry in st.session_state.product_entries]
                    account_ids = [entry['account_id'] for entry in st.session_state.product_entries if entry['account_id'] is not None]
                    return_ids = [entry['return_id'] for entry in st.session_state.product_entries if entry['return_id'] is not None]

                    # Use the single PO Number for all products in the transaction
                    po_numbers = [po_number] * len(product_ids)

                    # Use the combined date & time for the entire transaction
                    add_transaction(trans_type_id[0], product_ids, quantities, account_ids, return_ids, po_numbers, transaction_datetime)
                    st.success('Transaction recorded successfully!')
                    st.session_state.product_entries = []
                    push_to_github()


        elif option == 'Nuwe Produk':
            st.header('Add a New Product')
            product_name = st.text_input('Product Name')
            if st.button('Add Product'):
                add_product(product_name)
                st.success('Product added successfully!')
            
            # Display all products in a table
            products = load_products()
            df_products = pd.DataFrame(products, columns=['Product_ID', 'Product'])
            
            st.write("All Products:")
            
            if st.session_state['is_editor']:
                edited_df = st.data_editor(df_products, num_rows="dynamic")

                # Save changes to the database
                if st.button("Save Changes"):
                    for index, row in edited_df.iterrows():
                        update_product(row['Product_ID'], row['Product'])
                    st.success("Changes saved successfully!")
            else:
                st.table(df_products)

        elif option == 'Report':
            st.header('Product Balances')
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("Balans"):
                    st.session_state.view_mode = 'Balans'
            with col2:
                if st.button("Transaksie"):
                    st.session_state.view_mode = 'Transaksie'
            with col3:
                if st.button("Recon"):
                    st.session_state.view_mode = 'Recon'

            # Check the current view mode and display corresponding data
            view_mode = st.session_state.view_mode

            if view_mode == 'Balans':
                balances = get_balances()
                df_balances = pd.DataFrame(balances, columns=['Product', 'Balance'])
                st.write("Current Product Balances:")
                st.table(df_balances)

#Transaction Report

            elif view_mode == 'Transaksie':
                st.header('Transaction Details')
                
                # Fetch transactions
                transactions = get_transactions()
                
                if not transactions:
                    st.warning("No transactions found.")
                else:
                    # Convert transactions to DataFrame

                    st.write("Transaction Details:")
                    
                    if st.session_state['is_editor']:
                        edit = st.radio('Edit:', ('No', 'Delete', 'Edit'), key='edit_mode')
                        
                        if edit == 'Delete':
                            df_transactions = pd.DataFrame(transactions, columns=['Trans_ID', 'Product', 'TransType', 'Quantity', 'Date', 'Account', 'Name', 'ReturnType', 'PONumber', 'weekNo'])
                            
                            # Display the dataframe with selections enabled
                            selected_rows = st.dataframe(
                                df_transactions,
                                use_container_width=True,
                                hide_index=True,
                                on_select="rerun",
                                selection_mode="single-row"
                            ).selection.rows
                            
                            if selected_rows:
                                selected_row = df_transactions.iloc[selected_rows[0]]
                                selected_transaction_id = selected_row['Trans_ID']
                                selected_product_name = selected_row['Product']
                                selected_product_id = get_product_id_by_name(selected_product_name)

                                if selected_product_id is not None:
                                    selected_product_id = int(selected_product_id)
                                    selected_transaction_id = int(selected_transaction_id)  # Convert to int
                                    
                                    if st.button("Delete Selected Transaction"):
                                        delete_transaction(selected_transaction_id, selected_product_id)
                                        st.success(f"Deleted Transaction_ID: {selected_transaction_id} and Product_ID: {selected_product_id}")
                                        time.sleep(1)
                                        st.rerun()  # Refresh the page after deletion
                                else:
                                    st.error("Failed to retrieve the Product ID.")
                    
                        elif edit == 'Edit':
                            df_transactions_edit = pd.DataFrame(get_transactions_edit(),columns=['Trans_ID', 'Product', 'Product_ID', 'TransType', 'Quantity', 'Date', 'Account', 'Name', 'ReturnType', 'PONumber', 'weekNo'])
                # Display the dataframe with inline editing enabled
                            edited_df = st.data_editor(df_transactions_edit, num_rows="dynamic", key='transactions_editor')

                            # Collect the updated rows
                            updated_rows = []
                            for index, row in edited_df.iterrows():
                                if not row.equals(df_transactions_edit.loc[index]):
                                    updated_rows.append(row)

                            # Button to save changes
                            if st.button("Save Changes"):
                                for row in updated_rows:
                                    transaction_id = row['Trans_ID']
                                    product_id = row['Product_ID']
                                    trans_type_id = get_trans_type_id_by_name(row['TransType'])
                                    quantity = row['Quantity']
                                    date = row['Date']
                                    account_id = get_account_id_by_name(row['Account'])
                                    return_id = get_return_id_by_name(row['ReturnType'])
                                    po_numbers = row['PONumber']
                                    update_transaction(transaction_id, product_id, trans_type_id, account_id, date, quantity, return_id, po_numbers)
                                    st.write(f"Trans_ID: {transaction_id}, Product_ID: {product_id}")
                                time.sleep(1)
                                st.success("Changes saved successfully!")
                                st.rerun()  # Refresh the page after update


                    
                    if not st.session_state['is_editor'] or edit == 'No':
                        df_transactions = pd.DataFrame(transactions, columns=['Trans_ID', 'Product', 'TransType', 'Quantity', 'Date', 'Account', 'Name', 'ReturnType', 'PONumber', 'weekNo'])
                        # Filters
                        filter_columns = ['Product', 'Name', 'Account', 'TransType', 'ReturnType', 'PONumber', 'weekNo']
                        filters = add_filters(df_transactions, filter_columns)
                        df_filtered = apply_filters(df_transactions, filters)

                        # Sorting and Pagination
                        df_filtered = df_filtered.sort_values(by='Date', ascending=False)
                        df_paginated = paginate_dataframe(df_filtered, page_size=10)

                        st.write("Filtered Transactions:")
                        st.dataframe(df_paginated)





            elif view_mode == 'Recon':
                st.write("Recon View:")
                recon_data = get_recon_data()
                df_recon = pd.DataFrame(recon_data, columns=['Product', 'transdate_Start', 'transdate_End', 'Qty_In', 'Qty_Uit', 'Qty_Returned', 'Previous Balance', 'Latest Balance', 'Deviation',])
                df_recon = add_week_numbers(df_recon, 'transdate_End')

                st.write("Product Recon Balances:")

                # Filters
                filter_columns = ['Product', 'weekNo']
                filters = add_filters(df_recon, filter_columns)
                df_filtered = apply_filters(df_recon, filters)

                # Sorting and Pagination
                df_filtered = df_filtered.sort_values(by='transdate_End', ascending=False)
                df_paginated = paginate_dataframe(df_filtered, page_size=10)

                st.table(df_paginated)

    else:
        st.warning('Please log in to access this section.')

    # Add the download link for the database file
    st.markdown(download_database('stock_control.db'), unsafe_allow_html=True)

def add_filters(df, filter_columns):
    filters = {}
    clear_filters = st.button("Clear Filters")

    if clear_filters:
        for col in filter_columns:
            st.session_state[f"filter_{col}"] = "All"
        st.rerun()

    num_filters_per_row = 4
    rows = (len(filter_columns) + num_filters_per_row - 1) // num_filters_per_row  # Calculate the number of rows needed

    for row in range(rows):
        cols = st.columns(num_filters_per_row)
        for idx in range(num_filters_per_row):
            col_idx = row * num_filters_per_row + idx
            if col_idx < len(filter_columns):
                col = filter_columns[col_idx]
                with cols[idx]:
                    unique_values = ["All"] + list(df[col].fillna('None').unique())
                    if f"filter_{col}" not in st.session_state:
                        st.session_state[f"filter_{col}"] = "All"
                    filters[col] = st.selectbox(f"{col}", unique_values, key=f"filter_{col}")

    return filters

    return filters

def apply_filters(df, filters):
    for col, value in filters.items():
        if value != "All":
            if value == 'None':
                df = df[df[col].isna()]
            else:
                df = df[df[col] == value]
    return df

def add_week_numbers(df, date_column):
    df['weekNo'] = pd.to_datetime(df[date_column]).dt.isocalendar().week
    return df

def paginate_dataframe(df, page_size):
    total_rows = len(df)
    page = st.number_input('Page', min_value=1, max_value=(total_rows // page_size) + 1, value=1)
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    return df[start_index:end_index]

# Run the app
if __name__ == "__main__":
    main()
