#==================================================
# TÍTULO: pipe_compass.py
# DESCRIÇÃO: Aplicativo de console que realiza pipeline de dados para estudo da Fast Track da Compass UOL.
#
# ==================== ARGs =======================
# Exemplo de uso:
#   python %pipe% -r 
#==================================================

# ========== Variáveis de Identificação =========
nome = 'pipe_compass'
versao = '1.0'
data = '2025-08-16'

# ========== Importações ============
import sys
import src.py.extract_api as uptAPI

# Ao digitar somente o comando "python %pipe%" sem uasr argumentos exibe texto descritivo com as opçoes
if len(sys.argv) < 2:

    print("\n======pipe_compass - Pipeline de Dados========")
    print("Versão: {} | Data: {}".format(versao, data))
    print("Descrição: Aplicativo de console que realiza pipelines de dados para \nestudo da Fast Track de Engenharia de Dados da Compass UOL.")
    print("==============================================\n")

    print("Uso:")
    print("  python pipe_compass.py -[opção] [extra]\n")
    
    print("Opções disponíveis:")
    print("  -r api1    : Atualizar dados da API 1")
    print("  -r api2    : Atualizar dados da API 2")
    
    print("\nExemplo de execução:")
    print("  python %pipe% -r api1")
    print("==============================================")

    exit(0)

if sys.argv[1] == '-r':
    
    #---- Acessa função da API 1 ------
    if sys.argv [2] == 'api1':
        upg = uptAPI.extractAPI()
        upg.updateAPI1()

    #---- Acessa função da API 2 ------
    if sys.argv [2] == 'api2':
        upg = uptAPI.extractAPI()
        upg.updateAPI2()

else:
    print('Opção ' + sys.argv[1] + ' não encontrado.')