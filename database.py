import sqlite3
from datetime import datetime
import hashlib
import streamlit as st

# Function to connect to the SQLite database
def create_connection(db_file='stock_control.db'):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    return conn

# Common function to execute a query and fetch all results
def execute_query_fetch_all(conn, query, params=()):
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        return cur.fetchall()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    return []

# Function to load return types
def load_return_types():
    conn = create_connection('stock_control.db')
    return execute_query_fetch_all(conn, 'SELECT Returns_ID, ReturnType FROM tbl_Returns')

# Function to add a new product
def add_product(product_name):
    conn = create_connection('stock_control.db')
    if conn:
        try:
            with conn:
                sql = '''INSERT INTO tbl_Product(Product) VALUES(?)'''
                cur = conn.cursor()
                cur.execute(sql, (product_name,))
                conn.commit()
                return cur.lastrowid
        except sqlite3.Error as e:
            print(f"An error occurred while adding product: {e}")
        finally:
            conn.close()
    return None

# Function to add a transaction
def add_transaction(trans_type_id, product_ids, quantities, account_ids=None, return_ids=None):
    conn = create_connection('stock_control.db')
    customer_id = st.session_state.get('user_id')  # Get the logged-in user's ID from session state
    if not customer_id:
        raise ValueError("No logged-in user found")

    if conn:
        try:
            with conn:
                transaction_sql = '''INSERT INTO tbl_Transaction(TransType_ID_FK, DateTime, Customer_ID_FK) VALUES(?,?,?)'''
                cur = conn.cursor()
                cur.execute(transaction_sql, (trans_type_id, datetime.now(), customer_id))
                transaction_id = cur.lastrowid

                if trans_type_id in [1, 3]:  # 'In' or 'Stock Take'
                    product_transaction_sql = '''INSERT INTO tbl_ProductTransaction(Transaction_ID_FK, Product_ID_FK, Qty) VALUES(?,?,?)'''
                    cur.executemany(product_transaction_sql, [(transaction_id, pid, qty) for pid, qty in zip(product_ids, quantities)])
                elif trans_type_id == 2:  # 'Uit'
                    product_transaction_sql = '''INSERT INTO tbl_ProductTransaction(Transaction_ID_FK, Product_ID_FK, Account_ID_FK, Qty) VALUES(?,?,?,?)'''
                    cur.executemany(product_transaction_sql, [(transaction_id, pid, aid, qty) for pid, aid, qty in zip(product_ids, account_ids, quantities)])
                elif trans_type_id == 4:  # 'Return'
                    product_transaction_sql = '''INSERT INTO tbl_ProductTransaction(Transaction_ID_FK, Product_ID_FK, Account_ID_FK, Qty, Return_ID_FK) VALUES(?,?,?,?,?)'''
                    cur.executemany(product_transaction_sql, [(transaction_id, pid, aid, qty, rid) for pid, aid, qty, rid in zip(product_ids, account_ids, quantities, return_ids)])
                
                conn.commit()
                return transaction_id
        except sqlite3.Error as e:
            print(f"An error occurred while adding transaction: {e}")
        finally:
            conn.close()
    return None


# Function to load accounts
def load_accounts():
    conn = create_connection('stock_control.db')
    return execute_query_fetch_all(conn, 'SELECT Account_ID, AccountName FROM tbl_Accounts')

# Function to load transaction types
def load_trans_types():
    conn = create_connection('stock_control.db')
    return execute_query_fetch_all(conn, 'SELECT TransType_ID, TransType FROM tbl_TransType')

# Function to load products
def load_products():
    conn = create_connection('stock_control.db')
    return execute_query_fetch_all(conn, 'SELECT Product_ID, Product FROM tbl_Product ORDER BY Product ASC')

def load_data():
    return load_trans_types(), load_products()

# Function to get balances
def get_balances():
    conn = create_connection('stock_control.db')
    if conn:
        try:
            query = '''
            WITH LatestStocktake AS (
                SELECT pt.Product_ID_FK, 
                       MAX(tr.DateTime) AS LatestStocktakeDate
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN tbl_TransType tt ON tr.TransType_ID_FK = tt.TransType_ID
                WHERE tt.TransType = 'Stock Take'
                GROUP BY pt.Product_ID_FK
            ),
            StocktakeBalance AS (
                SELECT pt.Product_ID_FK,
                       SUM(pt.Qty) AS StocktakeQty
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN LatestStocktake lst ON pt.Product_ID_FK = lst.Product_ID_FK
                                         AND tr.DateTime = lst.LatestStocktakeDate
                GROUP BY pt.Product_ID_FK
            ),
            TransactionsAfterStocktake AS (
                SELECT pt.Product_ID_FK,
                       SUM(CASE WHEN t.TransType = 'In' THEN pt.Qty ELSE 0 END) AS Qty_In,
                       SUM(CASE WHEN t.TransType = 'Uit' THEN -pt.Qty ELSE 0 END) AS Qty_Out,
                       SUM(CASE WHEN t.TransType = 'Return' AND r.ReturnType = 'Hergebruik' THEN pt.Qty ELSE 0 END) AS Qty_Return
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN tbl_TransType t ON tr.TransType_ID_FK = t.TransType_ID
                LEFT JOIN tbl_Returns r ON pt.Return_ID_FK = r.Returns_ID
                WHERE tr.DateTime > (SELECT MAX(tr2.DateTime) FROM tbl_Transaction tr2 
                                     JOIN tbl_TransType tt2 ON tr2.TransType_ID_FK = tt2.TransType_ID
                                     WHERE tt2.TransType = 'Stock Take' AND tr2.Customer_ID_FK = tr.Customer_ID_FK)
                GROUP BY pt.Product_ID_FK
            )
            SELECT p.Product,
                   COALESCE(sb.StocktakeQty, 0) +
                   COALESCE(ta.Qty_In, 0) +
                   COALESCE(ta.Qty_Return, 0) +
                   COALESCE(ta.Qty_Out, 0) AS Balance
            FROM tbl_Product p
            LEFT JOIN StocktakeBalance sb ON p.Product_ID = sb.Product_ID_FK
            LEFT JOIN TransactionsAfterStocktake ta ON p.Product_ID = ta.Product_ID_FK
            '''
            return execute_query_fetch_all(conn, query)
        finally:
            conn.close()
    return []

# Function to get transactions
def get_transactions():
    conn = create_connection('stock_control.db')
    query = '''
    SELECT 
        pt.Transaction_ID_FK AS Transaction_ID,
        p.Product, 
        t.TransType, 
        pt.Qty, 
        tr.DateTime, 
        a.AccountName, 
        c.CustomerName || ' ' || c.CustomerSurname AS Name,
        r.ReturnType
    FROM 
        tbl_ProductTransaction pt
    JOIN 
        tbl_Product p ON pt.Product_ID_FK = p.Product_ID
    JOIN 
        tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
    JOIN 
        tbl_TransType t ON tr.TransType_ID_FK = t.TransType_ID
    LEFT JOIN 
        tbl_Customer c ON tr.Customer_ID_FK = c.Customer_ID
    LEFT JOIN 
        tbl_Accounts a ON pt.Account_ID_FK = a.Account_ID
    LEFT JOIN 
        tbl_Returns r ON pt.Return_ID_FK = r.Returns_ID
    '''
    return execute_query_fetch_all(conn, query)


# Function to get recon data
def get_recon_data():
    conn = create_connection('stock_control.db')
    if conn:
        try:
            query = '''
            WITH LatestStocktake AS (
                SELECT pt.Product_ID_FK, 
                       MAX(tr.DateTime) AS LatestStocktakeDate
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN tbl_TransType tt ON tr.TransType_ID_FK = tt.TransType_ID
                WHERE tt.TransType = 'Stock Take'
                GROUP BY pt.Product_ID_FK
            ),
            PreviousStocktake AS (
                SELECT pt.Product_ID_FK, 
                       MAX(tr.DateTime) AS PreviousStocktakeDate
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN tbl_TransType tt ON tr.TransType_ID_FK = tt.TransType_ID
                WHERE tt.TransType = 'Stock Take'
                AND tr.DateTime < (SELECT MAX(LatestStocktakeDate) FROM LatestStocktake WHERE Product_ID_FK = pt.Product_ID_FK)
                GROUP BY pt.Product_ID_FK
            ),
            StocktakeBalance AS (
                SELECT pt.Product_ID_FK,
                       SUM(pt.Qty) AS StocktakeQty,
                       tr.DateTime
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN tbl_TransType tt ON tr.TransType_ID_FK = tt.TransType_ID
                WHERE tt.TransType = 'Stock Take'
                GROUP BY pt.Product_ID_FK, tr.DateTime
            ),
            TransactionsInRange AS (
                SELECT pt.Product_ID_FK,
                       SUM(CASE WHEN t.TransType = 'In' THEN pt.Qty ELSE 0 END) as Qty_In,
                       SUM(CASE WHEN t.TransType = 'Uit' THEN pt.Qty ELSE 0 END) as Qty_Uit,
                       SUM(CASE WHEN t.TransType = 'Return' AND r.ReturnType = 'Hergebruik' THEN pt.Qty ELSE 0 END) AS Qty_Return
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN tbl_TransType t ON tr.TransType_ID_FK = t.TransType_ID
                LEFT JOIN tbl_Returns r ON pt.Return_ID_FK = r.Returns_ID
                JOIN LatestStocktake lst ON pt.Product_ID_FK = lst.Product_ID_FK
                JOIN PreviousStocktake pst ON pt.Product_ID_FK = pst.Product_ID_FK
                WHERE tr.DateTime > pst.PreviousStocktakeDate AND tr.DateTime <= lst.LatestStocktakeDate
                GROUP BY pt.Product_ID_FK
            )
            SELECT p.Product,
                   pst.PreviousStocktakeDate AS transdate_Start,
                   lst.LatestStocktakeDate AS transdate_End,
                   trir.Qty_In,
                   trir.Qty_Uit,
                   trir.Qty_Return,
                   COALESCE(pstb.StocktakeQty, 0) AS PreviousBalance,
                   COALESCE(lstb.StocktakeQty, 0) AS LatestBalance,
                   -1 * (COALESCE(pstb.StocktakeQty, 0) + COALESCE(trir.Qty_In, 0) - COALESCE(trir.Qty_Uit, 0) + COALESCE(trir.Qty_Return, 0) - COALESCE(lstb.StocktakeQty, 0)) AS Deviation
            FROM tbl_Product p
            LEFT JOIN LatestStocktake lst ON p.Product_ID = lst.Product_ID_FK
            LEFT JOIN PreviousStocktake pst ON p.Product_ID = pst.Product_ID_FK
            LEFT JOIN StocktakeBalance pstb ON pst.Product_ID_FK = pstb.Product_ID_FK AND pst.PreviousStocktakeDate = pstb.DateTime
            LEFT JOIN StocktakeBalance lstb ON lst.Product_ID_FK = lstb.Product_ID_FK AND lst.LatestStocktakeDate = lstb.DateTime
            LEFT JOIN TransactionsInRange trir ON p.Product_ID = trir.Product_ID_FK
            WHERE lst.LatestStocktakeDate IS NOT NULL
            '''
            return execute_query_fetch_all(conn, query)
        finally:
            conn.close()
    return []


# Function to add a new customer
def add_customer(name, surname, email, hashed_password):
    conn = create_connection('stock_control.db')
    if conn:
        try:
            with conn:
                sql = '''INSERT INTO tbl_Customer(CustomerName, CustomerSurname, Email, Password) VALUES(?,?,?,?)'''
                cur = conn.cursor()
                cur.execute(sql, (name, surname, email, hashed_password))
                conn.commit()
                return cur.lastrowid
        except sqlite3.Error as e:
            print(f"An error occurred while adding customer: {e}")
        finally:
            conn.close()
    return None

# Function to validate login
def validate_login(email, password):
    conn = create_connection('stock_control.db')
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('SELECT Customer_ID, Password, CustomerName, CustomerSurname FROM tbl_Customer WHERE Email = ?', (email,))
            row = cur.fetchone()
            if row and hashlib.sha256(password.encode()).hexdigest() == row[1]:
                return row[0], row[2], row[3]  # Return user_id, name, and surname
        except sqlite3.Error as e:
            print(f"An error occurred while validating login: {e}")
        finally:
            conn.close()
    return None, None, None


# Function to hash passwords
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def get_user_full_name(email):
    conn = create_connection()
    user = conn.execute('''
        SELECT CustomerName, CustomerSurname FROM tbl_Customer WHERE Email = ?
    ''', (email,)).fetchone()
    return user if user else (None, None)

# Function to update a transaction
def update_transaction(transaction_id, product_id, trans_type_id, qty):
    conn = create_connection('stock_control.db')
    if conn:
        try:
            with conn:
                sql = '''
                UPDATE tbl_ProductTransaction
                SET Product_ID_FK = ?, Qty = ?
                WHERE Transaction_ID_FK = ?
                '''
                cur = conn.cursor()
                cur.execute(sql, (product_id, qty, transaction_id))
                conn.commit()

                sql = '''
                UPDATE tbl_Transaction
                SET TransType_ID_FK = ?
                WHERE Transaction_ID = ?
                '''
                cur.execute(sql, (trans_type_id, transaction_id))
                conn.commit()
        except sqlite3.Error as e:
            print(f"An error occurred while updating transaction: {e}")
        finally:
            conn.close()

def update_product(product_id, product_name):
    conn = create_connection('stock_control.db')
    if conn:
        try:
            with conn:
                sql = '''UPDATE tbl_Product SET Product = ? WHERE Product_ID = ?'''
                cur = conn.cursor()
                cur.execute(sql, (product_name, product_id))
                conn.commit()
        except sqlite3.Error as e:
            print(f"An error occurred while updating product: {e}")
        finally:
            conn.close()
