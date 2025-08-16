# Introdução
Este arquivo possui a documentação do sistema de pipeline de estudo da Fast Track de Engenharia de Dados da Compass.

# Requisitos
Para rodar este programa, é necessário:

- Ter instalado o python 3.13+.
- Instalar as bibliotecas listadas no arquivo **requirements.txt**.
- Criar variáveis de ambiente.

# Estrutura

---
│   changelog.txt                                   : log de modificações
│   README.md                                       : Instruções gerais de execução
│   requirements.txt                                : Dependências da aplicação
│   pipe_compass.py                                 : Módulo principal que coordena a execução do programa via console
│   __init__.py                                     : Mapeia os arquivos .py da pasta o qual o Python reconhece a pasta como um pacote
│
└───src                                             : Todos arquivos utilizados
    ├───py                                          : Todos arquivos de python utilizados
    │   __init__.py                                 : Mapeia os arquivos .py da pasta o qual o Python reconhece a pasta como um pacote
    │   extract_api.py                              : Atualizador de APIs
    │   transform.py                                : Transformações dos dados

└─── output                                         : Todos arquivos com bases de dados exportadas

└─── tests                                          : Notebooks utilizados para testes

## Configurando as Variáveis de Ambiente

O pipe_compass necessita da algumas variáveis de ambiente que precisam ser configuradas antes de rodar o programa.

Para criar uma nova variável, basta seguir as duas etapas abaixo.

1. Acesse _Painel de Controle -> Contas de Usuário -> Alterar Variáveis do Meu Ambiente (à esquerda)_.
2. Nas variáveis de usuário, clique em **Novo** e digite o nome e o valor da variável.

*OBS: O texto abaixo indica NOME_VARIAVEL=VALOR_VARIAVEL*
pipe=c:\camininho\pipe_compass.py                                : Caminho do serverplan_console.py

# Publicando uma nova versão

Sempre que for desenvolver alguma nova função ou correção de erros, é necessário criar uma nova branch no repositório para tal.  
Após o desenvolvimento, deverá ser aberto um pull request e feito o merge com a branch principal.  
O changelog e o número da versão com a data no arquivo pipe_compass.py devem ser preenchidos.

