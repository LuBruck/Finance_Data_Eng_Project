import pandas as pd
import numpy as np

def _extract_sheets(caminho : str, sheets_detail : list):
    """
    Deve passar uma lista de dict com as seguintes info:
    list = [
    {
        sheet_name="sheet_name",
        header = 1,
        usecols = "B:N"
        },
    ]
    
    """
    planilha = pd.ExcelFile(caminho)
    dts = []
    for sheet in sheets_detail:
        sheet_name = sheet.get("sheet_name")
        header = sheet.get("header", 0)
        usecols = sheet.get("usecols", None)

        if sheet_name:
            dt = planilha.parse(sheet_name=sheet_name, header=header, usecols=usecols)
        else: 
            dt = planilha.parse(header=header, usecols=usecols)
        dts.append(dt)

    if len(dts) == 2:
        return dts[0], dts[1]
    return dts

    # dt_atletas = planilha.parse(sheet_name="Atletas", header=3 , usecols="B:N")
    # dt_associados = planilha.parse(sheet_name="Associados", header=3,usecols= "B:N")

    # return dt_atletas, dt_associados

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

    sheets = [
        {
            "sheet_name" : "Atletas",
            "header" : 3,
            "usecols" :"B:N"
        },
        {
            "sheet_name": "Associados",
            "header": 3,
            "usecols": "B:N"
        },
    ]
    dt_atleta_raw , dt_associado_raw = _extract_sheets(caminho_controle, sheets)

    dt_associado_long = _melt_monthly_fee(dt_associado_raw, "Associado")
    dt_atleta_long = _melt_monthly_fee(dt_atleta_raw, "Atleta")

    dt_atleta = _clean_monthly_payment(dt_atleta_long)
    dt_associado = _clean_monthly_payment(dt_associado_long)

    dt_mensalidade_out = pd.concat([dt_atleta, dt_associado])

    if caminho_out:
        dt_mensalidade_out.to_csv(path_or_buf=caminho_out)
    return dt_mensalidade_out

def transform_person_master(path_in , path_out):

    sheet = [
        {
            "sheet_name" : "Cadastro_Mestre",
            "header" : 0,
            "usecols" :"A:C"      
        },
        {
            "sheet_name" : "Times",
            "header" : 0,
            "usecols" :"A:D"  
        },
        {
            "sheet_name" : "Campeonatos",
            "header" : 0,
            "usecols" :"A:F"  
        }
    ]
    sheets = _extract_sheets(path_in,sheet)

    for dt in sheets:
        dt.replace('', pd.NA, inplace = True)
        dt.dropna(how='all',inplace=True)
        if "Nome" in dt.columns:
            dt["Nome"] = dt["Nome"].str.strip().str.replace(r'\s+', ' ', regex=True)

            dt["CPF"] = (
                dt["CPF"]
                .str.replace('.', '', regex=False)
                .str.replace('-', '', regex=False)
                .str.strip()
                .str.zfill(11)
            )

    dt_person_master = sheets[0]
    dt_team_person = sheets[1]
    dt_championship = sheets[2]

    dt_person_master['Primeiro Nome'] = dt_person_master['Nome'].str.split().str[0]
    dt_person_master['Ultimo Nome'] = dt_person_master['Nome'].str.split().str[-1]
    dt_person_master['Categoria'] = dt_person_master['Categoria'].str.replace(' ', '', regex= False)
    dt_team_person['Tipo - Time'] = dt_team_person['Tipo - Time'].str.split().str[0]

    conds = [
        dt_team_person['Time'].str.contains('AS', na=False),
        dt_team_person['Time'].str.contains('C', na=False) 
    ]

    choices = [
        dt_team_person["Time"].str.split(" - ").str[0],    
        "COED " + dt_team_person["Time"].str.split().str[0]
    ]
    
    dt_team_person["Nivel do time"] = np.select(conds, choices, default='')

    dt_team_person["Nome Time"] = np.where(
        dt_team_person['Time'].str.contains('C'),
        dt_team_person["Time"].str.split(" - ").str[1],
        ''
    )
    dt_team_person.drop(columns=["Time"], inplace = True)
    if path_out:
        dt_person_master.to_csv(path_or_buf=f'{path_out}person_master')
        dt_team_person.to_csv(path_or_buf=f'{path_out}team_person')
        dt_championship.to_csv(path_or_buf=f'{path_out}championship_team')
    return dt_person_master, dt_team_person


if __name__ == "__main__":
    # dt_mensalidade_out = transform_monthly_fee_data("data/ControleMensalidades.xlsx", "out/ControleMensalidade")

    # print(dt_mensalidade_out)

    dt_person_master = transform_person_master(
        "data/dim_person_master.xlsx",
        "out/"
    )


