import streamlit as st
from database import create_connection, add_product, add_transaction, load_products, load_trans_types, load_data, get_balances, get_transactions, get_recon_data, add_customer, validate_login, hash_password, get_user_full_name, load_accounts
import pandas as pd

if 'product_entries' not in st.session_state:
    st.session_state.product_entries = [{'product_id': None, 'qty': 1}]

# Initialize session state
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'user_id' not in st.session_state:
    st.session_state['user_id'] = None
if 'customer_name' not in st.session_state:
    st.session_state['customer_name'] = ""
if 'customer_surname' not in st.session_state:
    st.session_state['customer_surname'] = ""

def load_user_credentials():
    conn = create_connection('stock_control.db')
    try:
        cur = conn.cursor()
        cur.execute('SELECT Email, Password FROM tbl_Customer')
        rows = cur.fetchall()
        credentials = {'usernames': {}}
        for row in rows:
            credentials['usernames'][row[0]] = {'name': row[0], 'password': row[1]}
        return credentials
    finally:
        conn.close()

# Sidebar menu based on login state
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
        user_id, customer_name, customer_surname = validate_login(login_email, login_password)
        if user_id:
            st.session_state['user_id'] = user_id
            st.session_state['logged_in'] = True
            st.session_state['customer_name'] = customer_name
            st.session_state['customer_surname'] = customer_surname
            st.success('Login successful!')
            st.experimental_rerun()
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
        if signup_password == signup_password_confirm:
            hashed_password = hash_password(signup_password)
            user_id = add_customer(signup_name, signup_surname, signup_email, hashed_password)
            if user_id:
                st.success('You have successfully signed up! Please log in.')
                st.experimental_rerun()
            else:
                st.error('Error signing up. Please try again.')
        else:
            st.error('Passwords do not match')

elif option == 'Logout':
    st.session_state['logged_in'] = False
    st.session_state['user_id'] = None
    st.session_state['customer_name'] = ""
    st.session_state['customer_surname'] = ""
    st.success('Logged out successfully!')
    st.experimental_rerun()

elif 'logged_in' in st.session_state and st.session_state['logged_in']:
    if option == 'Transaksie':
        st.header('Create a Transaction')
        trans_types, product_list = load_data()
        account_list = load_accounts()
        trans_type_id = st.selectbox('Transaction Type', trans_types, format_func=lambda x: x[1])

        # Buttons to add or remove products
        col1, col2 = st.columns(2)
        with col1:
            if st.button('Add another product'):
                st.session_state.product_entries.append({'product_id': None, 'qty': 1, 'account_id': None})
        with col2:
            if st.button('Remove last product') and len(st.session_state.product_entries) > 1:
                st.session_state.product_entries.pop()

        # Dynamically manage product selections
        with st.form("transaction_form"):
            for i, entry in enumerate(st.session_state.product_entries):
                cols = st.columns([3, 1, 3])
                with cols[0]:
                    entry['product_id'] = st.selectbox('Select Product', product_list, format_func=lambda x: x[1], key=f"product_{i}")
                with cols[1]:
                    entry['qty'] = st.number_input('Quantity', value=entry['qty'], min_value=0, key=f"qty_{i}")
                if trans_type_id[1] == 'Uit':  # Check if transaction type is 'Uit'
                    with cols[2]:
                        account_names = [account[1] for account in account_list]
                        selected_account = st.radio('Select Account', account_names, key=f"account_{i}")
                        entry['account_id'] = [account[0] for account in account_list if account[1] == selected_account][0]
                else:
                    entry['account_id'] = None

            # Form submission button
            submitted = st.form_submit_button("Submit Transaction")
            if submitted:
                product_ids = [entry['product_id'][0] for entry in st.session_state.product_entries]
                quantities = [entry['qty'] for entry in st.session_state.product_entries]
                account_ids = [entry['account_id'] for entry in st.session_state.product_entries if entry['account_id'] is not None]
                add_transaction(trans_type_id[0], product_ids, quantities, account_ids)
                st.success('Transaction recorded successfully!')
                st.session_state.product_entries = []

    elif option == 'Nuwe Produk':
        st.header('Add a New Product')
        product_name = st.text_input('Product Name')
        if st.button('Add Product'):
            add_product(product_name)
            st.success('Product added successfully!')

    elif option == 'Report':
        st.header('Product Balances')
        view_mode = st.radio("View", ['Balans', 'Transaksie', 'Recon'])

        if view_mode == 'Balans':
            balances = get_balances()
            df_balances = pd.DataFrame(balances, columns=['Product', 'Balance'])
            st.write("Current Product Balances:")
            st.table(df_balances)
        elif view_mode == 'Transaksie':
            transactions = get_transactions()
            df_transactions = pd.DataFrame(transactions, columns=['Product', 'Transaction Type', 'Quantity', 'Date', 'Account', 'Name'])
            st.write("Transaction Details:")
            st.table(df_transactions)
        elif view_mode == 'Recon':
            st.write("Recon View:")
            recon_data = get_recon_data()
            df_recon = pd.DataFrame(recon_data, columns=['Product', 'transdate_Start', 'transdate_End', 'Qty_In', 'Qty_Uit', 'Previous Balance', 'Latest Balance', 'Deviation'])
            st.write("Product Recon Balances:")
            st.table(df_recon)

else:
    st.warning('Please log in to access this section.')
