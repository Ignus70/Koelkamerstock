import streamlit as st
from database import create_connection, add_product, add_transaction, load_products, load_trans_types, load_data, get_balances, get_transactions, get_recon_data
import pandas as pd

trans_types, product_list = load_data()

# Initialize state for product entries
if 'product_entries' not in st.session_state:
    st.session_state.product_entries = [{'product_id': None, 'qty': 1}]

st.sidebar.title('Menu')
option = st.sidebar.selectbox('Choose option:', ['Transaksie', 'Report', 'Nuwe Produk'])

if option == 'Transaksie':
    st.header('Create a Transaction')
    trans_type_id = st.selectbox('Transaction Type', trans_types, format_func=lambda x: x[1])

    # Buttons to add or remove products
    col1, col2 = st.columns(2)
    with col1:
        if st.button('Add another product'):
            st.session_state.product_entries.append({'product_id': None, 'qty': 1})
    with col2:
        if st.button('Remove last product') and len(st.session_state.product_entries) > 1:
            st.session_state.product_entries.pop()

    # Dynamically manage product selections
    with st.form("transaction_form"):
        for i, entry in enumerate(st.session_state.product_entries):
            cols = st.columns([3, 1])
            with cols[0]:
                entry['product_id'] = st.selectbox('Select Product', product_list, format_func=lambda x: x[1], key=f"product_{i}")
            with cols[1]:
                entry['qty'] = st.number_input('Quantity', value=entry['qty'], min_value=1, key=f"qty_{i}")
        
        # Form submission button
        submitted = st.form_submit_button("Submit Transaction")
        if submitted:
            product_ids = [entry['product_id'][0] for entry in st.session_state.product_entries]
            quantities = [entry['qty'] for entry in st.session_state.product_entries]
            add_transaction(trans_type_id[0], product_ids, quantities)
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
        df_transactions = pd.DataFrame(transactions, columns=['Product', 'Transaction Type', 'Quantity', 'Date'])
        st.write("Transaction Details:")
        st.table(df_transactions)   
    elif view_mode == 'Recon':
        st.write("Recon View:")
        recon_data = get_recon_data()
        df_recon = pd.DataFrame(recon_data, columns=['Product', 'transdate_Start', 'transdate_End', 'Qty_In', 'Qty_Uit', 'Previous Balance', 'Latest Balance', 'Deviation'])
        st.write("Product Recon Balances:")
        st.table(df_recon)