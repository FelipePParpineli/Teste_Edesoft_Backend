from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
import re
import pandas as pd
from datetime import datetime


# Inicializa o app
app = Flask(__name__)

# Configura o Banco de Dados
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///teste_edesoft.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class CessaoFundo(db.Model):
    """Classe que define as models (COLUNAS E TABELA)"""

    __tablename__ = 'CESSAO_FUNDO'

    ID_CESSAO = db.Column(db.Integer, nullable=False, primary_key=True, autoincrement=True)
    ORIGINADOR = db.Column(db.String(250), nullable=False)
    DOC_ORIGINADOR = db.Column(db.Integer, nullable=False)
    CEDENTE = db.Column(db.String(250), nullable=False)
    DOC_CEDENTE = db.Column(db.Integer, nullable=False)
    CCB = db.Column(db.Integer, nullable=False)
    ID_EXTERNO = db.Column(db.Integer, nullable=False)
    CLIENTE = db.Column(db.String(250), nullable=False)
    CPF_CNPJ = db.Column(db.String(50), nullable=False)
    ENDERECO = db.Column(db.String(250), nullable=False)
    CEP = db.Column(db.String(50), nullable=False)
    CIDADE = db.Column(db.String(250), nullable=False)
    UF = db.Column(db.String(50), nullable=False)
    VALOR_DO_EMPRESTIMO = db.Column(db.Numeric(10,2), nullable=False)
    VALOR_PARCELA = db.Column(db.Numeric(10,2), nullable=False)
    TOTAL_PARCELAS = db.Column(db.Integer, nullable=False)
    PARCELA = db.Column(db.Integer, nullable=False)
    DATA_DE_EMISSAO = db.Column(db.Date, nullable=False)
    DATA_DE_VENCIMENTO = db.Column(db.Date, nullable=False)
    PRECO_DE_AQUISICAO = db.Column(db.Numeric(10,2), nullable=False)

    def __init__(self, ORIGINADOR, DOC_ORIGINADOR, CEDENTE, DOC_CEDENTE, CCB, ID_EXTERNO, CLIENTE, CPF_CNPJ, ENDERECO, CEP, CIDADE, UF, VALOR_DO_EMPRESTIMO, VALOR_PARCELA, TOTAL_PARCELAS, PARCELA, DATA_DE_EMISSAO, DATA_DE_VENCIMENTO, PRECO_DE_AQUISICAO):

        self.ORIGINADOR = ORIGINADOR
        self.DOC_ORIGINADOR = DOC_ORIGINADOR
        self.CEDENTE = CEDENTE
        self.DOC_CEDENTE = DOC_CEDENTE
        self.CCB = CCB
        self.ID_EXTERNO = ID_EXTERNO
        self.CLIENTE = CLIENTE
        self.CPF_CNPJ = CPF_CNPJ
        self.ENDERECO = ENDERECO
        self.CEP = CEP
        self.CIDADE = CIDADE
        self.UF = UF
        self.VALOR_DO_EMPRESTIMO = VALOR_DO_EMPRESTIMO
        self.VALOR_PARCELA = VALOR_PARCELA
        self.TOTAL_PARCELAS = TOTAL_PARCELAS
        self.PARCELA = PARCELA
        self.DATA_DE_EMISSAO = DATA_DE_EMISSAO
        self.DATA_DE_VENCIMENTO = DATA_DE_VENCIMENTO
        self.PRECO_DE_AQUISICAO = PRECO_DE_AQUISICAO

# Cria a rota para fazer o POST
@app.route('/lambda/<bucket_name>/<object_key>', methods=['GET', 'POST'])
def lambda_handler(bucket_name, object_key):
    """Função que recebe os parâmetros via POST e executa, acessa arquivo .csv, trata as informações e insere no Banco de Dados"""

    if request.method == 'POST' or request.method == 'GET':
        try:
            # Acessa o arquivo .csv no bucket
            url = f'https://{bucket_name}.s3.amazonaws.com/{object_key}'
            df = pd.read_csv(url, encoding='iso-8859-1', sep=';')
            
            # Lista para inserir as instancias (INSERTS DO BANCO DE DADOS)
            lista_instancias = []

            # Faz o loop para iterar nas informações do aqruivo .csv
            for item in df.iterrows():

                # Cria uma instancia de cada vez
                instancia_db = CessaoFundo(item[1]['Originador'], item[1]['Doc Originador'], item[1]['Cedente'], item[1]['Doc Cedente'], item[1]['CCB'], item[1]['Id'], item[1]['Cliente'], item[1]['CPF/CNPJ'], item[1]['Endereço'], item[1]['CEP'], item[1]['Cidade'], item[1]['UF'], float(re.sub(r'[A-Z]', '', item[1]['Valor do Empréstimo']).replace(',', '.')), float(re.sub(r'[A-Z]', '', item[1]['Parcela R$']).replace(',', '.')), item[1]['Total Parcelas'], item[1]['Parcela #'], [datetime.strptime(item[1]['Data de Emissão'],'%d/%m/%Y').date()][0], [datetime.strptime(item[1]['Data de Vencimento'], '%d/%m/%Y').date()][0], float(item[1]['Preço de Aquisição']))
                lista_instancias.append(instancia_db)
            
            # Insere todas as linhas de uma só vez no Banco de Dados
            with app.app_context():
                for item in lista_instancias:
                    # Cria a sessão com o Banco de Dados
                    db.session.add(item)
                # Salva as alterações no Banco de Dados
                db.session.commit()
            return 'Sucesso - Banco de Dados Atualizado'
        
        except Exception as erro:
            return 'Falha ao atualizar Banco de Dados'

if __name__ == "__main__":
    app.run()
