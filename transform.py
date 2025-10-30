import pandas as pd
import numpy as np

def _is_valid_year(value: str, min_year: int , max_year: int) -> bool:
    try:
        y = int(value)
    except (TypeError, ValueError):
        return False
    return min_year <= y <= max_year

def _ask_year(prompt: str, min_year : int = 2020, max_year: int = 2028) -> int:
    while True:
        val = input(prompt).strip()
        if _is_valid_year(val, min_year, max_year):
            return val
        print(f"Ano inválido. Informe um inteiro entre {min_year} e {max_year}.")

def _extract_sheets(caminho : str, sheets_detail : list):
    """
    Deve passar uma lista de dict com as seguintes info:
    list = [
    {
        "sheet_name" : "sheet_name",
        "header" : 1,
        "usecols" : "B:N"
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


def _melt_monthly_fee(dt_raw : pd.DataFrame, categoria : str, year : str):

    meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", 
             "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro" ]
    
    dt = dt_raw.melt(
        id_vars="Nome",
        var_name="Mes",
        value_vars=meses,
        value_name="Pagamento"
        )
    dt["Categoria"] = categoria

    month_to_date_map = {
        mes.lower(): f"{str(i+1).zfill(2)}/{year}"
        for i, mes in enumerate(meses)
    }

    dt["Mes"] = dt["Mes"].str.lower().map(month_to_date_map)

    return dt

def _clean_monthly_payment(dt : pd.DataFrame):
    def separa_val(valor):
        if isinstance(valor, (int, float)) and valor > 0:
            return valor, "pago"
        elif pd.isna(valor):
                return 0, "nao pago"
        elif str(valor).strip().lower() == '':
            return 0, "nao pago"
        elif valor == 0:
            return 0, "nao pago"
        else:
            return 0, str(valor).strip().lower()

    dt[["Valor", "Status"]] = dt["Pagamento"].apply(
    lambda x:pd.Series(separa_val(x)))
    
    dt.drop(columns=["Pagamento"], inplace=True)

    dt["Nome"] = dt["Nome"].str.strip().str.replace(r'\s+', ' ', regex=True)
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


    # year = _ask_year("Whith year this monthly fee control is from?")
    year = "2025"
    dt_associado_long = _melt_monthly_fee(dt_associado_raw, "Associado", year)
    dt_atleta_long = _melt_monthly_fee(dt_atleta_raw, "Atleta", year)

    dt_atleta = _clean_monthly_payment(dt_atleta_long)
    dt_associado = _clean_monthly_payment(dt_associado_long)

    dt_mensalidade_out = pd.concat([dt_atleta, dt_associado])

    if caminho_out:
        dt_mensalidade_out.to_csv(path_or_buf=caminho_out)
    return dt_mensalidade_out

def transform_person_master(path_in:str , path_out:str):

    r"""
    Parameters
    ---
    path_out : The path_out in this case is just the directory
    """

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
    
    dt_team_person["Nivel_time"] = np.select(conds, choices, default='')

    dt_team_person["Nome_Time"] = np.where(
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


def _melt_individual_data(df_raw : pd.DataFrame):

    if 'Categoria De Pagamento' in df_raw.columns:
        df_interval = df_raw.loc[:, 'Categoria De Pagamento':'Total Arrecadado'].drop(
            columns=['Categoria De Pagamento', 'Total Arrecadado'])
    else:
        df_interval = df_raw.loc[:, 'Time':'Total'].drop(columns=['Time', 'Total'])

    value_vars = df_interval.columns
    df_melted = df_raw.melt(
        id_vars=["Nome", "CPF"],
        var_name="source",
        value_name="value",
        value_vars=value_vars
    )

    df_melted['date'] = df_melted["source"].str.split(' - ').str[-1]
    df_melted['source'] = df_melted["source"].str.split(' - ').str[0]
    df_melted['value'] = df_melted["value"].fillna(0)
    df_melted = df_melted.dropna()

    return df_melted

def transform_individual_cash(path_in:str, path_out: str):


    sheets = [
        {
            "sheet_name" : "Dados",
            "header" : 1,
        },
        {
            "sheet_name": "Pagamento Individual",
            "header": 1,
        },
        ]
    df_individual = _extract_sheets(path_in, sheets)

    for df in df_individual:
        df["Nome"] = df["Nome"].str.strip().str.replace(r'\s+', ' ', regex=True)
        df["CPF"] = (
                df["CPF"]
                .str.replace('.', '', regex=False)
                .str.replace('-', '', regex=False)
                .str.strip()
                .str.zfill(11)
            )
    
    events_payment = _melt_individual_data(df_individual[0])

    individual_payment = df_individual[1].loc[:, 'Nome':'Total Arrecadado']
    individual_payment = _melt_individual_data(individual_payment)

    df_individual = pd.concat([events_payment, individual_payment])

    df_individual.to_csv(path_out)
    return df_individual


    
if __name__ == "__main__":
    dt_mensalidade_out = transform_monthly_fee_data("data/ControleMensalidades.xlsx", "out/ControleMensalidade")

    # print(dt_mensalidade_out)

    dt_person_master = transform_person_master(
        "data/dim_person_master.xlsx",
        "out/"
    )

    transform_individual_cash("data/Caixa_Individual.xlsx", "out/individual_cash")

    aaa = pd.DataFrame()