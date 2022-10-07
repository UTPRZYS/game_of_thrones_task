import sqlite3

connection = sqlite3.connect("game_of_thrones.db")
cursor = connection.cursor()

# delete 
#cursor.execute("""DROP TABLE employee;""")

sql_command = """
CREATE TABLE books ( 
url varchar(255) PRIMARY KEY, 
first_name VARCHAR(20), 
last_name VARCHAR(30), 
region VARCHAR(30), 
country VARCHAR(30),
street VARCHAR(30),
postal_code VARCHAR(30),
created_date DATE);"""

cursor.execute(sql_command)

sql_command = """INSERT INTO customer (id, first_name, last_name, region, country, street, postal_code, created_date)
    VALUES (NULL, "Andrzej", "Kowalski", "Europe", "Poland", "Kakaowa 2", "04-234", "1997-08-17");"""
cursor.execute(sql_command)

connection.commit()

connection.close()
