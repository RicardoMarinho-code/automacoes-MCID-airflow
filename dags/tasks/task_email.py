import os
import sys
import io
import paramiko
import pandas as pd
import unicodedata
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Float, Integer, Date
from dotenv import load_dotenv

# Extract execution date passed from Airflow or fallback to current local time
exec_dt = pd.to_datetime(sys.argv[1]) if len(sys.argv) > 1 else pd.Timestamp.now()
LAST_EXECTION_DATE = exec_dt.hour + 1
NEXT_EXECTION_DATE = exec_dt.hour + 3

# Load environment variables from .env file
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../.env'))
load_dotenv(dotenv_path=env_path)

# Database credentials from .env
db_host     = os.environ.get('host')
db_port     = os.environ.get('port')
db_database = os.environ.get('database')
db_user     = os.environ.get('user')
db_password = os.environ.get('password')

# SFTP credentials from .env
sftp_host     = os.environ.get('SFTP_HOST')
sftp_port     = int(os.environ.get('SFTP_PORT'))
sftp_user     = os.environ.get('SFTP_USER')
sftp_password = os.environ.get('SFTP_PASSWORD')
sftp_dir      = os.environ.get('SFTP_DIR')
sftp_dir_anteriores = os.environ.get('SFTP_DIR_ANTERIORES')

file_prefix = os.environ.get('file_prefix')

local_dir = os.environ.get('local_dir')

TABLE_NAME = os.environ.get('TABLE_NAME')
BANCO_BRUTOS = os.environ.get('BANCO_BRUTOS')
BANCO_PROCESSADOS = os.environ.get('BANCO_PROCESSADOS')


def get_already_processed_files():
    """Returns the set of filenames already loaded into the database."""
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}'
        )
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT DISTINCT _source_file FROM {BANCO_PROCESSADOS}.{TABLE_NAME}")
            )
            return {row[0] for row in result.fetchall()}
    except Exception:
        return set()


def list_new_csv_files(sftp):
    """Lists CSV files in the remote directory that start with the target prefix."""
    files = sftp.listdir(sftp_dir)
    return [
        f for f in files
        if f.startswith(file_prefix) and f.endswith(".csv")
    ]


def download_and_parse(sftp, filename):
    """Downloads a CSV file from SFTP and returns a Pandas DataFrame."""
    remote_path = os.path.join(sftp_dir, filename).replace('\\', '/')
    buffer = io.BytesIO()
    sftp.getfo(remote_path, buffer)
    buffer.seek(0)
    df = pd.read_csv(buffer, sep=';', encoding='utf-8', on_bad_lines='skip')
    return df


def load_to_db(df, source_filename):
    """Loads a DataFrame into both the brutos and processados PostgreSQL tables."""
    df.columns = [
        unicodedata.normalize('NFKD', str(c)).encode('ASCII', 'ignore').decode('utf-8').lower().replace(' ', '_')
        for c in df.columns
    ]
    
    df['_source_file'] = source_filename
    engine = create_engine(
        f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}'
    )
    
    # Truncate tables before inserting new data
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {BANCO_BRUTOS}.{TABLE_NAME}"))
            conn.execute(text(f"TRUNCATE TABLE {BANCO_PROCESSADOS}.{TABLE_NAME}"))
            print(f"[INFO] Tables {BANCO_BRUTOS}.{TABLE_NAME} and {BANCO_PROCESSADOS}.{TABLE_NAME} truncated successfully.")
    except Exception as e:
        print(f"ERROR AT TRUNCATING TABLES: {e} \n\n")
        
    # all columns as VARCHAR
    df_brutos = df.astype(str)
    dtype_brutos = {col: String(255) for col in df_brutos.columns}
    
    try:
        df_brutos.to_sql(
            TABLE_NAME,
            engine,
            schema=BANCO_BRUTOS,
            if_exists='append',
            index=False,
            dtype=dtype_brutos
        )
        print(f"[OK] Loaded {len(df_brutos)} rows into {BANCO_BRUTOS}.{TABLE_NAME}")
    except Exception as e:
        print(f"[ERROR] Failed to load into {BANCO_BRUTOS}: {e}")

    df_processados = df.copy()
    
    try:
        df_processados['data_de_movimento'] = pd.to_datetime(df_processados.get('data_de_movimento'), dayfirst=True, errors='coerce')
        df_processados['data_de_contratacao'] = pd.to_datetime(df_processados.get('data_de_contratacao'), dayfirst=True, errors='coerce')
        
        if 'valor_contratado' in df_processados.columns and not pd.api.types.is_numeric_dtype(df_processados['valor_contratado']):
            df_processados['valor_contratado'] = df_processados['valor_contratado'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    except Exception as e:
        print(f"[WARNING] Some issues parsing dates/floats, continuing anyway: {e}")
    dtype_processados = {
        'data_de_movimento': Date(),
        'agente_financeiro': String(255),
        'apf': String(255),
        'uf': String(2),
        'municipio': String(255),
        'codigo_ibge_do_municipio': String(255),
        'nome_empreendimento': String(255),
        'modalidade': String(255),
        'data_de_contratacao': Date(),
        'valor_contratado': Float(),
        'uh_contratadas': Integer()
    }
    
    try:
        df_processados.to_sql(
            TABLE_NAME,
            engine,
            schema=BANCO_PROCESSADOS,
            if_exists='append',
            index=False,
            dtype=dtype_processados
        )
        print(f"[OK] Loaded {len(df_processados)} rows into {BANCO_PROCESSADOS}.{TABLE_NAME}")
    except Exception as e:
        print(f"[ERROR] Failed to load into {BANCO_PROCESSADOS}: {e}")


def run():
    os.makedirs(local_dir, exist_ok=True)

    # Connect to SFTP server (WinSCP-compatible SFTP)
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_user, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        available_files = list_new_csv_files(sftp)
        if not available_files:
            print("[INFO] No files matching prefix on SFTP server. Nothing to do.")
            return

        # The filename ends with YYYYMMDD, so alphabetical max == most recent.
        latest = max(available_files)
        processed_files = get_already_processed_files()

        if processed_files == {latest}:
            print(f"[INFO] DB already loaded with {latest}. Nothing to do.")
            return

        print(f"[INFO] Processing latest file: {latest}")
        try:
            df = download_and_parse(sftp, latest)
            load_to_db(df, latest)

            if sftp_dir_anteriores:
                old_path = os.path.join(sftp_dir, latest).replace('\\', '/')
                new_path = os.path.join(sftp_dir_anteriores, latest).replace('\\', '/')
                try:
                    sftp.rename(old_path, new_path)
                    print(f"[INFO] Moved '{latest}' to {sftp_dir_anteriores}")
                except IOError as e:
                    print(f"[WARN] Could not move '{latest}' to {sftp_dir_anteriores}: {e}")
        except Exception as e:
            print(f"[ERROR] Failed to process '{latest}': {e}")
    finally:
        sftp.close()
        transport.close()


if __name__ == '__main__':
    run()
