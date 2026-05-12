import os
import paramiko
import pandas as pd
from dotenv import load_dotenv
import io

load_dotenv('.env')

sftp_host     = os.environ.get('SFTP_HOST')
sftp_port     = int(os.environ.get('SFTP_PORT', 22))
sftp_user     = os.environ.get('SFTP_USER')
sftp_password = os.environ.get('SFTP_PASSWORD')
sftp_dir      = os.environ.get('SFTP_DIR', '/')

filename = 'Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260505.csv'

transport = paramiko.Transport((sftp_host, sftp_port))
transport.connect(username=sftp_user, password=sftp_password)
sftp = paramiko.SFTPClient.from_transport(transport)

remote_path = os.path.join(sftp_dir, filename).replace('\\', '/')
buffer = io.BytesIO()
sftp.getfo(remote_path, buffer)
buffer.seek(0)
df = pd.read_csv(buffer, sep=';', encoding='utf-8', on_bad_lines='skip')

import unicodedata
df.columns = [
    unicodedata.normalize('NFKD', str(c)).encode('ASCII', 'ignore').decode('utf-8').lower().replace(' ', '_')
    for c in df.columns
]

df_processados = df.copy()

print("Is object?:", df_processados['valor_contratado'].dtype == object)
print("Dtype:", df_processados['valor_contratado'].dtype)

try:
    if 'valor_contratado' in df_processados.columns and df_processados['valor_contratado'].dtype == object:
        df_processados['valor_contratado'] = df_processados['valor_contratado'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
        print("Replaced!")
    else:
        print("Did not enter if block!")
except Exception as e:
    print("Error:", e)

print("Head after if:", df_processados['valor_contratado'].head())

sftp.close()
transport.close()
