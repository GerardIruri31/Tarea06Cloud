
import os, csv, tempfile
import boto3
import mysql.connector
from mysql.connector import Error

# Lee variables de entorno
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "demo")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "root")
MYSQL_TABLE= os.getenv("MYSQL_TABLE", "people")

S3_BUCKET  = os.getenv("S3_BUCKET")            # obligatorio
S3_KEY     = os.getenv("S3_KEY", f"{MYSQL_TABLE}.csv")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

def query_to_csv(cursor, rows, headers):
    # crea un archivo temporal CSV y devuelve su ruta
    fd, path = tempfile.mkstemp(prefix=f"{MYSQL_TABLE}_", suffix=".csv")
    os.close(fd)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    return path

def main():
    if not S3_BUCKET:
        raise SystemExit("Falta S3_BUCKET en variables de entorno.")

    print(f"Conectando a MySQL {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}…")
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
        user=MYSQL_USER, password=MYSQL_PASS
    )
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {MYSQL_TABLE}")
        rows = cursor.fetchall()
        headers = [d[0] for d in cursor.description]
        print(f"Filas leídas: {len(rows)}")

        csv_path = query_to_csv(cursor, rows, headers)
        print(f"CSV generado: {csv_path}")

        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.upload_file(csv_path, S3_BUCKET, S3_KEY)
        print(f"Subido a s3://{S3_BUCKET}/{S3_KEY}")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
        print("Ingesta completada ✅")
    except Error as e:
        print(f"Error MySQL: {e}")
        raise
    except Exception as e:
        print(f"Error general: {e}")
        raise