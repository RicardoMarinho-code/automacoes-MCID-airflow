import pandas as pd
df = pd.DataFrame({'valor_contratado': ['22.530.200,00', '10.164.000,00', None]})
try:
    df['valor_contratado'] = df['valor_contratado'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    print("Success:")
    print(df['valor_contratado'])
    print("Dtype:", df['valor_contratado'].dtype)
except Exception as e:
    print("Exception:", type(e), e)
