import os
import pandas as pd
import zipfile
import requests
import re
# import google.generativeai as genai


class DadosTse:
    def __init__(self):

        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.pasta = os.path.join(script_dir, 'arquivos_extraidos')

        # Gemini API
        # self.API_KEY = 'AIzaSyDxRKgbO8UawN0OH0foxpbvgPtkFDdPM5E'
        # genai.configure(api_key=self.API_KEY)
        # self.generation_config = {'temperature': 0.9, 'top_p': 1, 'top_k': 1, 'max_output_tokens': 2048}
        # self.model = genai.GenerativeModel('gemini-2.0-flash', generation_config=self.generation_config)

        self.estados = {
            'AC': 'Acre',
            'AL': 'Alagoas',
            'AP': 'Amapá',
            'AM': 'Amazonas',
            'BA': 'Bahia',
            'CE': 'Ceará',
            'DF': 'Distrito Federal',
            'ES': 'Espírito Santo',
            'GO': 'Goiás',
            'MA': 'Maranhão',
            'MT': 'Mato Grosso',
            'MS': 'Mato Grosso do Sul',
            'MG': 'Minas Gerais',
            'PA': 'Pará',
            'PB': 'Paraíba',
            'PR': 'Paraná',
            'PE': 'Pernambuco',
            'PI': 'Piauí',
            'RJ': 'Rio de Janeiro',
            'RN': 'Rio Grande do Norte',
            'RS': 'Rio Grande do Sul',
            'RO': 'Rondônia',
            'RR': 'Roraima',
            'SC': 'Santa Catarina',
            'SP': 'São Paulo',
            'SE': 'Sergipe',
            'TO': 'Tocantins'
        }

    def download_arquivos(self, ano, uf_estado: str, partido: list = '', candidato: list = ''):

        self.nome_candidato = [candidato] if type(
            candidato) == str and len(candidato) > 0 else candidato

        self.csv_files = []
        self.ano = str(ano)
        self.uf_estado = uf_estado.upper()
        self.partido = [partido] if type(
            partido) == str and len(partido) > 0 else partido

        uf_estado = self.uf_estado.upper()

        # Criação da self.pasta

        if not os.path.exists(self.pasta):

            os.makedirs(self.pasta)
        os.makedirs(
            os.path.join(self.pasta, self.ano, self.estados[uf_estado]), exist_ok=True)
        path = os.path.join(self.pasta, self.ano, self.estados[uf_estado])

        headers = {
            'User-Agent': 'Mozilla/5.0'
        }

        # Links
        link_votacao_secao = f'https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_secao/votacao_secao_{self.ano}_{uf_estado}.zip'
        link_eleitorado_local = f'https://cdn.tse.jus.br/estatistica/sead/odsele/eleitorado_locais_votacao/eleitorado_local_votacao_{self.ano}.zip'
        link_consulta_cand = f'https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_{self.ano}.zip'

        links = [link_votacao_secao, link_eleitorado_local, link_consulta_cand]
        print('=~' * 96)
        print()

        for index, link in enumerate(links, start=1):
            try:
                nome_arquivo = os.path.basename(link)
                eleitorado_true = f'eleitorado_local_votacao_{self.ano}.zip' in nome_arquivo
                caminho_arquivo = os.path.join(
                    self.pasta, self.ano, nome_arquivo) if eleitorado_true else os.path.join(path, nome_arquivo)

                print(f'\033[1;33;47m{index}º arquivo: {nome_arquivo}\033[m')

                if link == link_votacao_secao and f'votacao_secao_{self.ano}_{uf_estado}.csv' in os.listdir(path):
                    print('Arquivo votação seção já existe!')
                    self.csv_files.append(os.path.join(
                        path, f'votacao_secao_{self.ano}_{uf_estado}.csv'))
                    print()

                elif link == link_eleitorado_local and f'eleitorado_local_votacao_{self.ano}.csv' in os.listdir(os.path.join(self.pasta, self.ano)):
                    print('Arquivo eleitorado já existe!')
                    self.csv_files.append(os.path.join(
                        self.pasta, self.ano, f'eleitorado_local_votacao_{self.ano}.csv'))
                    print()

                elif link == link_consulta_cand and f'consulta_cand_{self.ano}_{uf_estado}.csv' in os.listdir(path):
                    print('Arquivo consulta candidato já existe!')
                    self.csv_files.append(
                        os.path.join(path, f'consulta_cand_{self.ano}_{uf_estado}.csv'))
                    print()

                else:
                    print('Baixando...')

                    response = requests.get(link, headers=headers)

                    with open(caminho_arquivo, 'wb') as arquivo:
                        arquivo.write(response.content)

                    print('Extraindo...')
                    with zipfile.ZipFile(caminho_arquivo, 'r') as zip_file:
                        for file in zip_file.namelist():
                            try:

                                if file == f'eleitorado_local_votacao_{self.ano}.csv':
                                    zip_file.extract(
                                        file, os.path.join(self.pasta, self.ano))
                                    self.csv_files.append(
                                        os.path.join(self.pasta, self.ano, file))

                                elif file == f'consulta_cand_{self.ano}_{uf_estado}.csv':
                                    zip_file.extract(file, path)
                                    self.csv_files.append(
                                        os.path.join(path, file))

                                elif file == f'votacao_secao_{self.ano}_{uf_estado}.csv':
                                    zip_file.extract(file, path)
                                    self.csv_files.append(
                                        os.path.join(path, file))

                            except Exception as e:
                                print(f'Erro ao extrair {file}: {e}')

                    os.remove(caminho_arquivo)
                    print('Zip removido:', nome_arquivo)
                    print()

            except Exception as e:
                print(e)

        print('=~' * 96)
        self.extracao_dados()

    def extracao_dados(self):
        lista_arquivos = self.csv_files
        '''
        - local_votacao = local_votacao.drop_duplicates(subset=['Zona', 'Secao']): vai manter apenas um valor independente do turno, pois o arquivo com os locais de votação duplicam a seção e zona quando existe 2º turno.
        '''

        try:

            votacao_secao = pd.read_csv(lista_arquivos[0], encoding='latin-1', sep=';', usecols=[
                                        'NM_VOTAVEL', 'QT_VOTOS', 'NR_ZONA', 'NR_SECAO', 'SQ_CANDIDATO', 'DS_CARGO'])
            local_votacao = pd.read_csv(lista_arquivos[1], encoding='latin-1', sep=';', usecols=[
                                        'NR_ZONA', 'NR_SECAO', 'NM_MUNICIPIO', 'NM_BAIRRO', 'SG_UF'])
            candidatos = pd.read_csv(lista_arquivos[2], encoding='latin-1', sep=';', usecols=[
                'SQ_CANDIDATO', 'NM_URNA_CANDIDATO', 'SG_PARTIDO', 'NR_TURNO', 'NM_UE'])

            local_votacao = local_votacao[local_votacao['SG_UF']
                                          == self.uf_estado]

            votacao_secao = votacao_secao.rename(columns={
                'NR_ZONA': 'Zona',
                'NR_SECAO': 'Secao'
            })
            local_votacao = local_votacao.rename(columns={
                'NR_ZONA': 'Zona',
                'NR_SECAO': 'Secao'
            })

            local_votacao = local_votacao.drop_duplicates(
                subset=['Zona', 'Secao'])

            dados_merge = pd.merge(
                votacao_secao, local_votacao, on=['Zona', 'Secao'])
            dados_merge = dados_merge[['Zona', 'Secao', 'NM_MUNICIPIO',
                                       'NM_BAIRRO', 'SQ_CANDIDATO', 'NM_VOTAVEL', 'DS_CARGO', 'QT_VOTOS']]

            dados_merge = dados_merge[~dados_merge['NM_VOTAVEL'].isin(
                ['VOTO NULO', 'VOTO BRANCO'])]

            # Candidatos
            candidatos = candidatos[candidatos['NR_TURNO'] == 1]

            dados_merge_completo = pd.merge(
                dados_merge, candidatos, on='SQ_CANDIDATO', how='left')
            dados_merge_completo = dados_merge_completo[['Zona', 'Secao', 'NM_MUNICIPIO', 'NM_BAIRRO',
                                                        'SQ_CANDIDATO', 'NM_VOTAVEL', 'NM_URNA_CANDIDATO', 'DS_CARGO', 'NM_UE', 'SG_PARTIDO', 'QT_VOTOS']]

            # dados_merge[dados_merge['SG_PARTIDO'] == 'NOVO']
            dados_merge_completo = dados_merge_completo.groupby(['NM_MUNICIPIO', 'NM_BAIRRO', 'NM_URNA_CANDIDATO', 'DS_CARGO', 'SG_PARTIDO', 'NM_UE'])[
                'QT_VOTOS'].sum().reset_index().sort_values(by='QT_VOTOS', ascending=False)
            dados_merge_completo.columns = [
                'Município', 'Bairro', 'Nome do candidato', 'Cargo', 'Sigla do partido', 'Local mandato', 'Votos']

            if len(self.partido) > 0:
                self.partido = [partido.upper() for partido in self.partido]
                dados_merge_completo = dados_merge_completo[dados_merge_completo['Sigla do partido'].isin(
                    self.partido)]

            if len(self.nome_candidato) > 0:
                self.nome_candidato = [candidato.upper()
                                       for candidato in self.nome_candidato]
                dados_merge_completo = dados_merge_completo[
                    dados_merge_completo['Nome do candidato'].isin(self.nome_candidato)]

            dados_merge_completo.to_csv(
                f'Eleições {self.ano} - {self.estados[self.uf_estado]}.csv.gz', compression='gzip', index=False, encoding='utf-8')
            # print(dados_merge_completo)

        except ValueError:
            print('Deu erro')
            pass

        # print(dados_merge_completo.groupby(['Nome do candidato', 'Cargo', 'Sigla do partido', 'Local mandato'])[
        #       'Votos'].sum().reset_index().sort_values(by='Votos', ascending=False).head(15))

    def detectar_candidato_eleicao(self, nome_urna: str):
        lista_csv = [os.path.join(root, file) for root, dirs, files in os.walk(self.pasta)
                     for file in files if file.startswith('consulta_cand_')]

        for lista in lista_csv:
            try:
                candidatos = pd.read_csv(os.path.join(self.pasta, lista), encoding='latin-1', sep=';', usecols=[
                    'SQ_CANDIDATO', 'NM_URNA_CANDIDATO', 'SG_PARTIDO', 'NR_TURNO', 'NM_UE'])

                candidatos = candidatos[candidatos['NM_URNA_CANDIDATO']
                                        == nome_urna.upper()]

                if len(candidatos) > 0:
                    print(f'\033[1;32mO candidato {nome_urna.title()} está presente no ano', re.search(
                        r'\d{4}', lista).group(0))
                else:
                    print(f'\033[m \033[1;31mO candidato {nome_urna.title()} não está presente no ano', re.search(
                        r'\d{4}', lista).group(0))
                    print('\033[m')

            except Exception as e:
                print(e)
                pass

    def comparacao_candidato_anos(self, anos: list, candidatos=''):
        """Comparação do(s) candidato(s) durante as eleições que participou(aram)"""
        anos = [anos] if type(anos) == int else [ano for ano in anos]
        candidatos = [candidatos.upper()] if type(candidatos) == str else [
            candidato.upper() for candidato in candidatos]
        df_anos = []

        for ano in anos:
            df = pd.read_csv(
                f'./{ano} ---/Eleições {ano} Rio de Janeiro.csv', encoding='latin-1')
            df['Ano'] = ano
            df['Ano'] = df['Ano'].astype('int32')
            df_anos.append(df)

        if df_anos:
            df_ano = pd.concat(df_anos)

            df_ano_agrupado = df_ano[df_ano['Nome do candidato'].isin(
                candidatos)]
            df_ano_agrupado = df_ano_agrupado.groupby(['Nome do candidato', 'Cargo', 'Ano'])[
                'Votos'].sum().reset_index().sort_values(by=['Ano', 'Nome do candidato'], ascending=False)
            print(df_ano_agrupado if len(df_ano_agrupado) >
                  0 else '\033[31mNão foi encontrado o candidato\033[m')

            # response = self.model.generate_content([f'Analise a variável {df_ano_agrupado} e me informa a porcentagem do ano em que tiveram menos e mais votos para cada candidato. Quero de forma mais resumida possível com a menor quantidade de linhas, só para eu entender. Coloque sem esses "*". Utilize "- ". Você pode utilizar o \033[m do Python para deixar os nomes com a cor verde pois estou em ambiente Python. Quero um resumo do que pedi antes. Nada de código em Python. Dê um espaço entre os nomes.'])
            # print(response.text)


if __name__ == '__main__':
    dados = DadosTse()

# dados.download_arquivos(2018, 'rj', candidato='eduardo paes')
# dados.comparacao_candidato_anos([2016,2018,2020,2024], ['leandro lyra', 'alexandre freitas'])
# dados.detectar_candidato_eleicao('eduardo paes')
# dados.detectar_candidato_eleicao('eduardo paes')


estados = {
    # 'AC': 'Acre',
    # 'AL': 'Alagoas',
    # 'AP': 'Amapá',
    # 'AM': 'Amazonas',
    # 'BA': 'Bahia',
    # 'CE': 'Ceará',
    # 'DF': 'Distrito Federal',
    # 'ES': 'Espírito Santo',
    # 'GO': 'Goiás',
    # 'MA': 'Maranhão',
    'MT': 'Mato Grosso',
    'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais',
    'PA': 'Pará',
    'PB': 'Paraíba',
    'PR': 'Paraná',
    'PE': 'Pernambuco',
    'PI': 'Piauí',
    'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia',
    'RR': 'Roraima',
    'SC': 'Santa Catarina',
    'SP': 'São Paulo',
    'SE': 'Sergipe',
    'TO': 'Tocantins'

}


# for key in estados.keys():
#     dados.download_arquivos(2016, key)
#     dados.download_arquivos(2018, key)
#     dados.download_arquivos(2020, key)
#     dados.download_arquivos(2022, key)
#     dados.download_arquivos(2024, key)

dados.download_arquivos(2022, 'pb')
