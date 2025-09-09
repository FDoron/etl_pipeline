import mysql.connector
from mysql.connector import Error
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.config_loader import Config

# Load config
config = Config.load("config/settings.yaml")
db_conf = config["db"]

conn = None  # initialize

try:
    conn = mysql.connector.connect(
        host=db_conf["host"],
        port=db_conf.get("port", 3306),
        user=db_conf["user"],
        password=db_conf["password"],
        database=db_conf["database"]
    )

    if conn.is_connected():
        print(f"Connected to MySQL database '{db_conf['database']}' successfully")
        cursor = conn.cursor()
        cursor.execute("SELECT NOW()")
        result = cursor.fetchone()
        print(f"Current DB time: {result[0]}")

except Error as e:
    print(f"Error connecting to MySQL: {e}")

finally:
    if conn and conn.is_connected():
        cursor.close()
        conn.close()
        print("Connection closed")
