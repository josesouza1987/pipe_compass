#==================================================
# TÍTULO: pipe_compass.py
# DESCRIÇÃO: Aplicativo de console que realiza pipeline de dados para estudo da Fast Track da Compass UOL.
#
# ==================== ARGs =======================
# Exemplo de uso:
#   python %pipe%
#==================================================

# ========== Variáveis de Identificação =========
nome = 'pipe_compass'
versao = '1.3'
data = '2025-08-23'

# ========== Importações ============
import sys
import src.py.extract_api as uptAPI

# Ao digitar somente o comando "python %pipe%" sem usar argumentos exibe texto descritivo com as opçoes
if len(sys.argv) < 2:

    print("\n======pipe_compass - Pipeline de Dados========")
    print("Versão: {} | Data: {}".format(versao, data))
    print("Descrição: Aplicativo de console que realiza pipelines de dados para \nestudo da Fast Track de Engenharia de Dados da Compass UOL.")
    print("==============================================\n")

    print("Uso:")
    print("  python pipe_compass.py -[opção] [extra1] [extra1]\n")
    
    print("Opções:")
    print("  -u updategeral        Atualizar todos os dados brutos e curados")
    print("  -u updateselic        Atualizar dados bruto da taxa selic do Banco do Brasil")
    print("  -u updatedolar        Atualizar dados bruto do valor do Dolar")    

    print("\nExemplo de execução:")
    print("  python %pipe% -u updadegeral")

    print("\nExemplo de execução especificando datas de início e fim no formato [aaaa-mm-dd]:")
    print("  python %pipe% -u updadegeral 2025-01-01 2025-12-31")

    print("==============================================")

    exit(0) # Pra encessar a execução do console

# Se o primeiro argumento após %pipe% for '-u'
if sys.argv[1] == '-u':
    
    #---- Atualiza todas bases de dados ------
    if sys.argv [2] == 'updategeral':
        upd = uptAPI.extractAPI()
        if len(sys.argv) == 5: # Checa se teve mais dois argumentos
            upd.dadosCurados(sys.argv[3], sys.argv[4])
        else:
            upd.dadosCurados()
            
    #---- Atualiza dados da taxa de selic do Banco do Brasil" ------
    if sys.argv [2] == 'updateselic':
        upd = uptAPI.extractAPI()
        if len(sys.argv) == 5: # Checa se teve mais dois argumentos
            upd.updateSelic(sys.argv[3], sys.argv[4])
        else:
            upd.updateSelic()

    #---- Atualiza base de dados de Dolar ------
    if sys.argv [2] == 'updatedolar':
        upd = uptAPI.extractAPI()
        if len(sys.argv) == 5: # Checa se teve mais dois argumentos
            upd.updateDolar(sys.argv[3], sys.argv[4])
        else:
            upd.updateDolar()

else:
    print('Opção ' + sys.argv[1] + ' não encontrado.')