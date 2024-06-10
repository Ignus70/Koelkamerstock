import sqlite3
from datetime import datetime

# Function to connect to the SQLite database
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    return conn

# Function to add a new product
def add_product(product_name):
    conn = create_connection('stock_control.db')
    with conn:
        sql = ''' INSERT INTO tbl_Product(Product)
                  VALUES(?) '''
        cur = conn.cursor()
        cur.execute(sql, (product_name,))
        conn.commit()
    return cur.lastrowid

# Function to add a transaction
def add_transaction(trans_type_id, product_ids, quantities):
    conn = create_connection('stock_control.db')
    with conn:
        # Insert transaction
        transaction_sql = ''' INSERT INTO tbl_Transaction(TransType_ID_FK, DateTime)
                              VALUES(?,?) '''
        cur = conn.cursor()
        cur.execute(transaction_sql, (trans_type_id, datetime.now()))
        transaction_id = cur.lastrowid
        
        # Insert product transactions
        for product_id, qty in zip(product_ids, quantities):
            product_transaction_sql = ''' INSERT INTO tbl_ProductTransaction(Transaction_ID_FK, Product_ID_FK, Qty)
                                          VALUES(?,?,?) '''
            cur.execute(product_transaction_sql, (transaction_id, product_id, qty))
        conn.commit()
    return transaction_id

def add_TransType(transType):
    conn = create_connection('stock_control.db')
    with conn:  # Using a context manager ensures that resources are cleaned up
        transaction_sql = ''' INSERT INTO tbl_TransType(TransType)
                              VALUES(?)'''  # Correct the table name and remove the extra comma
        cur = conn.cursor()
        cur.execute(transaction_sql, (transType,))  # Ensure tuple is correctly formatted
        conn.commit()
    return cur.lastrowid

# Load types and products for dropdowns
def load_trans_types():
    conn = create_connection('stock_control.db')
    cur = conn.cursor()
    cur.execute('SELECT TransType_ID, TransType FROM tbl_TransType')
    types = cur.fetchall()
    return types

def load_products():
    conn = create_connection('stock_control.db')
    cur = conn.cursor()
    cur.execute('SELECT Product_ID, Product FROM tbl_Product')
    products = cur.fetchall()
    return products

def load_data():
    conn = create_connection('stock_control.db')
    cur = conn.cursor()
    cur.execute('SELECT TransType_ID, TransType FROM tbl_TransType')
    types = cur.fetchall()
    cur.execute('SELECT Product_ID, Product FROM tbl_Product')
    products = cur.fetchall()
    return types, products

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
                JOIN tbl_TransType tt ON tr.TransType_ID_FK = tt.TransType_ID
                JOIN LatestStocktake lst ON pt.Product_ID_FK = lst.Product_ID_FK
                                         AND tr.DateTime = lst.LatestStocktakeDate
                GROUP BY pt.Product_ID_FK
            )
            SELECT p.Product,
                   COALESCE(sb.StocktakeQty, 0) +
                   SUM(CASE WHEN t.TransType = 'In' THEN pt.Qty
                            WHEN t.TransType = 'Uit' THEN -pt.Qty
                            ELSE 0 END) AS Balance
            FROM tbl_Product p
            LEFT JOIN tbl_ProductTransaction pt ON p.Product_ID = pt.Product_ID_FK
            LEFT JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
            LEFT JOIN tbl_TransType t ON tr.TransType_ID_FK = t.TransType_ID
            LEFT JOIN StocktakeBalance sb ON p.Product_ID = sb.Product_ID_FK
            WHERE tr.DateTime > COALESCE((SELECT MAX(lst.LatestStocktakeDate) FROM LatestStocktake lst WHERE lst.Product_ID_FK = p.Product_ID), '1970-01-01')
            OR t.TransType = 'Stock Take'
            GROUP BY p.Product
            '''
            cur = conn.cursor()
            cur.execute(query)
            balances = cur.fetchall()
            return balances
        except sqlite3.Error as e:
            print(f"An error occurred while getting balances: {e}")
        finally:
            conn.close()
    return []

def get_transactions():
    conn = create_connection('stock_control.db')
    query = '''
    SELECT p.Product, t.TransType, pt.Qty, tr.DateTime
    FROM tbl_ProductTransaction pt
    JOIN tbl_Product p ON pt.Product_ID_FK = p.Product_ID
    JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
    JOIN tbl_TransType t ON tr.TransType_ID_FK = t.TransType_ID
    '''
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()

def remove_transtype(trans_type_id):
    conn = create_connection('stock_control.db')
    with conn:
        sql = 'DELETE FROM tbl_TransType WHERE TransType_ID = ?'
        cur = conn.cursor()
        cur.execute(sql, (trans_type_id,))
        conn.commit()
    return cur.rowcount

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
                       SUM(CASE WHEN t.TransType = 'Uit' THEN pt.Qty ELSE 0 END) as Qty_Uit
                FROM tbl_ProductTransaction pt
                JOIN tbl_Transaction tr ON pt.Transaction_ID_FK = tr.Transaction_ID
                JOIN tbl_TransType t ON tr.TransType_ID_FK = t.TransType_ID
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
                   COALESCE(pstb.StocktakeQty, 0) AS PreviousBalance,
                   COALESCE(lstb.StocktakeQty, 0) AS LatestBalance,
                   (COALESCE(pstb.StocktakeQty, 0) + COALESCE(trir.Qty_In, 0) - COALESCE(trir.Qty_Uit, 0) - COALESCE(lstb.StocktakeQty, 0)) AS Deviation
            FROM tbl_Product p
            LEFT JOIN LatestStocktake lst ON p.Product_ID = lst.Product_ID_FK
            LEFT JOIN PreviousStocktake pst ON p.Product_ID = pst.Product_ID_FK
            LEFT JOIN StocktakeBalance pstb ON pst.Product_ID_FK = pstb.Product_ID_FK AND pst.PreviousStocktakeDate = pstb.DateTime
            LEFT JOIN StocktakeBalance lstb ON lst.Product_ID_FK = lstb.Product_ID_FK AND lst.LatestStocktakeDate = lstb.DateTime
            LEFT JOIN TransactionsInRange trir ON p.Product_ID = trir.Product_ID_FK
            WHERE lst.LatestStocktakeDate IS NOT NULL
            '''
            cur = conn.cursor()
            cur.execute(query)
            recon_data = cur.fetchall()
            processed_data = []
            for data in recon_data:
                product, transdate_start, transdate_end, qty_in, qty_uit, previous_balance, latest_balance, deviation = data
                processed_data.append((product, transdate_start, transdate_end, qty_in, qty_uit, previous_balance, latest_balance, deviation))
            return processed_data
        except sqlite3.Error as e:
            print(f"An error occurred while getting recon data: {e}")
        finally:
            conn.close()
    return []