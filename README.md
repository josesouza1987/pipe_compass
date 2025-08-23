# Introdução
Este arquivo possui a documentação do sistema de pipeline de estudo da Fast Track de Engenharia de Dados da Compass.

# Índice
1. [Estrutura](#estrutura)
2. [Requisitos](#requisitos)
3. [Instale as dependências](#instale-as-dependências)
4. [Configurando as Variáveis de Ambiente necessárias](#configurando-as-variáveis-de-ambiente-necessárias)
5. [Configurando o Agendador de Tarefas do Windows](#configurando-o-agendador-de-tarefas-do-windows)
6. [Publicando uma nova versão](#publicando-uma-nova-versão)
7. [Execução do programa](#execução-do-programa)
8. [Informações sobre as APIs utilizadas no projeto](#informações-sobre-as-apis-utilizadas-no-projeto)
9. [Explicação técnica da persistência](#explicação-técnica-da-persistência)

# Estrutura
```
│   __init__.py                                     : Mapeia os arquivos .py da pasta o qual o Python reconhece a pasta como um pacote
│   changelog.txt                                   : log de modificações
│   pipe_compass.py                                 : Módulo principal que coordena a execução do programa via console
│   README.md                                       : Instruções gerais de execução
│   requirements.txt                                : Dependências da aplicação
│
└───src                                             : Todos arquivos utilizados
    └───py                                          : Todos arquivos de python utilizados
        │   __init__.py                             : Mapeia os arquivos .py da pasta o qual o Python reconhece a pasta como um pacote
        └───extract_api.py                          : Atualizador de APIs
    │   
    └───notebooks                                   : Pasta contendo notebooks
        └───Relatório de Data Quality.ipnb          : Notebook com relatório com a qualidade de colunas dos dados da API
│
└─── output                                         : Todos arquivos de bases de dados exportadas
    │   curado                                      : Pasta contendo arquivos de base de dados com alto nível de tratamento prontos para uso
    └───dados_bruto                                 : Pasta contendo arquivos de base de dados brutos com nenhuma ou poucas transformações
│
└─── reports                                        : Pasta que contém relatórios de analise dos dados
        └─── Analise Financeira.pbix                : Arquivo de relatório do Power BI com indicadores do projeto
│
└─── tests                                          : Notebooks utilizados pra testes os quais não são relevantes para o fucnionamento do programa
```

# Requisitos
Para rodar este programa, é necessário:

- Ter instalado o python 3.13+.
- Instalar as bibliotecas listadas no arquivo **requirements.txt** o qual foi gerado sem especificar versão pois poderá ser a versão atual.
- Criar variáveis de ambiente.

# Instale as dependências 
Abra um terminal, acesse a pasta principal do projeto e execute o comando pip install -r requirements.txt

# Configurando as Variáveis de Ambiente necessárias
O pipe_compass necessita da algumas variáveis de ambiente que precisam ser configuradas antes de rodar o programa.

Para criar uma nova variável, basta seguir as duas etapas abaixo.

1. Acesse _Painel de Controle -> Contas de Usuário -> Alterar Variáveis do Meu Ambiente (à esquerda)_.
2. Nas variáveis de usuário, clique em **Novo** e digite o nome e o valor da variável.

```
*OBS: O texto abaixo indica NOME_VARIAVEL=VALOR_VARIAVEL*
pipe=c:\caminho_pasta\pipe_compass.py                                : Caminho do serverplan_console.py
output= c:\caminho_pasta\output                                      : Caminho da pasta que serão salvo os arquivos parquet
exchangerate_key=SUA_CHAVE_AQUI                                      : Chave de acesso da API Exchangerate.host
```
# Configurando o Agendador de Tarefas do Windows
O **pipe_compass** utiliza o **Agendador de Tarefas do Windows** para rodar os processos diários. 

As etapas pra configurá-lo são:

1. Abra o **Menu Iniciar** e procure por **Agendador de Tarefas**.

2. Clique com o botão direito em **Biblioteca do Agendador de Tarefas** e depois em **Criar Tarefa**.

Exemplo de configuração, Nome e Disparadores poderão serem alteradas conforme régra de negócio mas Ações deve seguir conforme abaixo.
Nome: atualizaPipeCompass 
Disparadores: Todos os dias às 07:00.
Ações:
    Programa/Script: escreva o caminho completo do arquvio pythonw.exe (fica localizado na pasta da instalação do python)
    Argumentos: %pipe% -u updategeral

# Publicando uma nova versão
Sempre que for desenvolver alguma nova função ou correção de erros, é necessário criar uma nova branch no repositório para tal.  
Após o desenvolvimento, deverá ser aberto um pull request e feito o merge com a branch principal.  
O changelog e o número da versão com a data no arquivo pipe_compass.py devem ser preenchidos.

# Execução do programa
Abra um terminal do sistema operacional e digite python %pipe%
Siga as instruções que aparecerão no terminal.

# Informações sobre as APIs utilizadas no projeto
A escolha dessas fontes de dados 

1. API taxa de juros Selic:
Objetivo: Obter os valores diários da taxa Selic, que é a taxa básica de juros da economia brasileira.

Documentação: https://dadosabertos.bcb.gov.br/dataset/11-taxa-de-juros---selic/resource/b73edc07-bbac-430c-a2cb-b1639e605fa8
Url: https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados
Autenticação: Não é necessária
Paginação: Não é necessário

Parâmetros: 
dataInicial = DD/MM/YYYY
dataFinal   = DD/MM/YYYY

Limitações: A diferença entre dataInicial e dataFinal não deve ser superior a 10 anos

Obs1.: para esse projeto está sendo utilizado dados históricos e dinamicos de -1 ano até a data atual.
Obs2.: é necessário realizar a multiplicação de 252 (dias de negociação) para dados relacionados ao mercado conforme a fonte: https://www.investopedia.com/terms/a/annualize.asp

2. API exchangerate.host de cotação do Dolar:
Objetivo: Obter cotações diárias do dólar em relação ao real (USD/BRL).

Documentação: https://exchangerate.host/documentation
Url: https://api.exchangerate.host/timeframe
Autenticação: deve passar a chave no parâmetro access_key o qual é fornecido ao realizar cadastro no site
Paginação: Não é necessário

Parâmetros: 
access_key = SUA_CHAVE
start_date = YYYY-MM-DD
end_date = YYYY-MM-DD
source = USD
currencies = BRL

Limitações: A diferença entre start_date e end_date não deve ser superior a 365 dias

Obs1.: para esse projeto está sendo utilizado dados históricos e dinamicos de -1 ano até a data atual.
Obs2.: o site fornece API grátis mas com limitação de não exceder 100 requisições no mês vigente.

# Explicação técnica da persistência
Formato escolhido: Utilizando .parquet que utiliza compactação a nível de colunas.

Particionamento: Está sendo particionado por colunas temporais com pastas nomeadas com ano(ex.: year-2025) e arquivos nomeados com número do mês(ex.: month-08.parquet).

Estrutura de pastas raízes: 
dados_bruto                                   : São dados brutos mas com tratamentos necessários
curados                                       : São arquivos com tratamentos/junções necessárias e prontos para consumo final