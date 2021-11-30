import sqlite3
import re


def create_northwind(): 
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor()
    with open("Northwind.Tables.sql", "r") as sql_file:
        sql_script = sql_file.read()
    c.executescript(sql_script)
    conn.commit()
    conn.close()


def select_seafood():
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor()

    c.execute("""
    SELECT ProductName, UnitPrice, UnitsInStock 
    FROM Products INNER JOIN Categories 
    ON Products.CategoryID = Categories.CategoryID
    WHERE CategoryName = "Seafood"
    """)
    for item in c.fetchall():
        print(f"Product: {item[0]}, price: {item[1]}, available: {item[2]}")

    conn.commit()
    conn.close()


def lookup_stats():
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor()
    
    c.execute("PRAGMA page_count")
    print(f"Page count: {c.fetchall()[0][0]}")

    c.execute("PRAGMA page_size")
    print(f"Page size: {c.fetchall()[0][0]}")

    c.execute("SELECT rootpage FROM sqlite_schema")
    print(f'Pages: {[item[0] for item in c.fetchall()]}')

    c.execute("PRAGMA synchronous")
    print(f"Synchronous mode: {c.fetchall()[0][0]}")

    c.execute("PRAGMA journal_mode")
    print(f"Journal mode: {c.fetchall()[0][0]}\n")

    conn.commit()
    conn.close()


def sqlite_schema():
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor()
    
    c.execute("SELECT name FROM sqlite_schema WHERE type = 'table'")
    print(f"Table list: {[tup[0] for tup in c.fetchall()]}")
    c.execute("SELECT rowid, name, sql FROM sqlite_schema WHERE type = 'table'")
    for item in c.fetchall():
        rowid = item[0]
        table_name = item[1]
        select_statement = re.sub(r"\n *|\t *|  *", r" ", item[2])
        select_statement = re.sub(r"(?<=,) ", r"\n", select_statement)
        print(f"Table {rowid}: {table_name}, select statement: {select_statement}\n")
    conn.commit()
    conn.close()


def get_table_info(table):
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    print(f"{table} table info: {c.fetchall()}")
    conn.commit()
    conn.close()


def get_create_statement(table):
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor()
    c.execute("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = (?)", (table,))
    statement = c.fetchall()[0][0]
    #statement = re.sub(r"\n *|\t *|  *", r" ", statement)
    #statement = re.sub(r"(?<=,) ", r"\n", statement)
    conn.commit()
    conn.close()
    return statement


def change_create_statement(table, statement):
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor() 
    c.execute("PRAGMA writable_schema = ON")
    c.execute(f"UPDATE sqlite_schema SET sql = '{statement}' WHERE type = 'table' AND name = (?)", (table,))
    conn.commit()
    conn.close()


def get_address():
    conn = sqlite3.connect('northwind.db')
    c = conn.cursor() 
    c.execute(f"SELECT address FROM Suppliers")
    result = c.fetchall()
    conn.commit()
    conn.close()
    return result


if __name__ == '__main__':
    create_northwind()
    #select_seafood()
    #lookup_stats()
    #sqlite_schema()
    #statement = get_create_statement("Suppliers")
    #print(f'Before changing address type to INTEGER: \n{statement}')
    #print(get_address())
    #statement = re.sub(r"(?<=\[Address\]).*?(?=,)", r"INTEGER", statement)
    #print(statement)
    #change_create_statement("Suppliers", statement)
    #statement = get_create_statement("Suppliers")
    #print(f'\nAfter changing address type to INTEGER: {statement}')
    #print(get_address())


