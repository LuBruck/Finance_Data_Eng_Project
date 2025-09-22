import pandas as pd
import numpy as np

def carrgar_planilha(caminho : str):
    
    planilha = pd.ExcelFile(caminho)

    dt_atletas = planilha.parse(sheet_name="Atletas", header=3 , usecols="B:N")
    dt_associados = planilha.parse(sheet_name="Associados", header=3,usecols= "B:N")

    return dt_atletas, dt_associados

def melt_dt(dt_raw : pd.DataFrame, categoria : str):
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

def limpar_dt(dt : pd.DataFrame):
    def separa_val(valor):
        if isinstance(valor, (int, float)):
            return valor, "pago"
        else:
            return np.nan, str(valor).strip().lower()

    dt[["Valor", "Status"]] = dt["Pagamento"].apply(
    lambda x:pd.Series(separa_val(x)))
    dt.drop(columns=["Pagamento"], inplace=True)
    return dt

def pipe_mensalidade(caminho_controle : str , caminho_out = ""):
    dt_atleta_raw , dt_associado_raw = carrgar_planilha(caminho_controle)

    dt_associado_long = melt_dt(dt_associado_raw, "Associado")
    dt_atleta_long = melt_dt(dt_atleta_raw, "Atleta")

    dt_atleta = limpar_dt(dt_atleta_long)
    dt_associado = limpar_dt(dt_associado_long)

    dt_mensalidade_out = pd.concat([dt_atleta, dt_associado])

    if caminho_out:
        dt_mensalidade_out.to_csv(path_or_buf=caminho_out)
    return dt_mensalidade_out


if __name__ == "__main__":
    dt_mensalidade_out = pipe_mensalidade("data/ControleMensalidades.xlsx", "out/ControleMensalidade")

    print(dt_mensalidade_out)