import pandas as pd
import numpy as np

def _extract_monthly_sheets(caminho : str):
    
    planilha = pd.ExcelFile(caminho)

    dt_atletas = planilha.parse(sheet_name="Atletas", header=3 , usecols="B:N")
    dt_associados = planilha.parse(sheet_name="Associados", header=3,usecols= "B:N")

    return dt_atletas, dt_associados

def _melt_monthly_fee(dt_raw : pd.DataFrame, categoria : str):
    meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", 
             "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    
    dt = dt_raw.melt(
        id_vars="Nome",
        var_name="Mes",
        value_vars=meses,
        value_name="Pagamento"
        )
    dt["Categoria"] = categoria

    return dt

def _clean_monthly_payment(dt : pd.DataFrame):
    def separa_val(valor):
        if isinstance(valor, (int, float)):
            return valor, "pago"
        else:
            return np.nan, str(valor).strip().lower()

    dt[["Valor", "Status"]] = dt["Pagamento"].apply(
    lambda x:pd.Series(separa_val(x)))
    dt.drop(columns=["Pagamento"], inplace=True)
    return dt

def transform_monthly_fee_data(caminho_controle : str , caminho_out = ""):
    dt_atleta_raw , dt_associado_raw = _extract_monthly_sheets(caminho_controle)

    dt_associado_long = _melt_monthly_fee(dt_associado_raw, "Associado")
    dt_atleta_long = _melt_monthly_fee(dt_atleta_raw, "Atleta")

    dt_atleta = _clean_monthly_payment(dt_atleta_long)
    dt_associado = _clean_monthly_payment(dt_associado_long)

    dt_mensalidade_out = pd.concat([dt_atleta, dt_associado])

    if caminho_out:
        dt_mensalidade_out.to_csv(path_or_buf=caminho_out)
    return dt_mensalidade_out

def transform_person_master(path_in , path_out):
    dim_person_master = pd.read_csv(path_in,sep= ';')
    dim_person_master["Nome"] = dim_person_master["Nome"].str.strip().str.replace(r'\s+', ' ', regex=True)

    dim_person_master["CPF"] = (
        dim_person_master["CPF"]
        .str.replace('.', '', regex=False)
        .str.replace('-', '', regex=False)
        .str.strip()
    )
    dim_person_master['Primeiro Nome'] = dim_person_master['Nome'].str.split().str[0]
    dim_person_master['Ultimo Nome'] = dim_person_master['Nome'].str.split().str[-1]

    dim_person_master["Nivel do time"] = np.where(
        dim_person_master['Categoria'].str.contains('C'),
        "COED " + dim_person_master["Categoria"].str.split().str[0],
        ''
    )

    dim_person_master["Nome Time"] = np.where(
        dim_person_master['Categoria'].str.contains('C'),
        dim_person_master["Categoria"].str.split(" - ").str[1],
        ''
    )

    dim_person_master["Categoria"] = np.where(
        dim_person_master['Categoria'].str.contains('C'),
        "Atleta",
        dim_person_master["Categoria"]
    )

    if path_out:
        dim_person_master.to_csv(path_or_buf=path_out)
    return dim_person_master

if __name__ == "__main__":
    # dt_mensalidade_out = transform_monthly_fee_data("data/ControleMensalidades.xlsx", "out/ControleMensalidade")

    # print(dt_mensalidade_out)

    dt_person_master = transform_person_master(
        "data/dim_person_master.csv",
        "out/person_master"
    )
    
    print(dt_person_master)
