import pandas as pd

mensalidade_Associados = pd.read_excel("data/ControleMensalidades.xlsx", header=3, sheet_name="Associados")
mensalidade = pd.read_excel("data/ControleMensalidades.xlsx", header=3).drop(columns=["Unnamed: 0"])

meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", 
         "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

mensalidade_melted = mensalidade.melt(id_vars=["Nome"], var_name="Mes", value_vars=meses, value_name="Valor")
associados_melted = mensalidade.melt(id_vars=["Nome"], var_name="Mes", value_vars=meses, value_name="Valor")

mensalidade_melted.to_csv("out/mensalidade_atletas")
associados_melted.to_csv("out/mensalidade_associados")
    