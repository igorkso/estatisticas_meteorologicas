#coding: utf-8 -*-

#analise estatistica para modelos meteorologicos diversos

import zarr
import gcsfs
import intake
import numpy as np
import pandas as pd
import xarray as xr
#import proplot as plot
import requests
import json
import os
import glob
import netCDF4 as nc
import ctypes
import zipfile
import cdsapi
import icclim
import warnings; warnings.filterwarnings(action='ignore')
from cdo import Cdo

def coleta_catalogo_cmip6():
    '''
    se conecta com a api do cmip6 e retorna um catalogo de informações como as de temperatura que utilizaremos para acessar os dados dos modelos do CMIP6 (https://www.wcrp-climate.org/wgcm-cmip/wgcm-cmip6)
    '''
    address = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    return intake.open_esm_datastore(address)


def model_keys(catalogo, experimento, tempo, variavel, idmembro):
    '''
    atributo >> catalogo - se refere ao que eh retornado pela funcao coleta_catalogo()
    atributo >> experimento - se refere ao id do experimento a ser estudado, no nosso caso usaremos 'historical'
    atributo >> tempo - se refere a que tipo de analise temporal sera feita (diaria, mensal, anual, etc)
    atributo >> variavel - esta relacionado com qual variavel meteorologica sera analisada.
    atributo >> idmembro - token de autenticacao para estabelecer comunicacao com a api.
    
    a funcao retorna um dataset com as chaves de acesso para os dataframes de temperatura maxima para a variavel escolhida. 'tasmax' se refere a temperatura maxima
    '''
    historical_tas = catalogo.search(experiment_id=[experimento], table_id=tempo, variable_id=[variavel], member_id=idmembro)
    tas = historical_tas.to_dataset_dict(zarr_kwargs={'consolidated': True}, storage_options={'token':'anon'})
    return tas


def modelo_cmip6(chave_modelo, tas):
    '''
    retorna um dataset com a chave do modelo a ser analisado. a lista de chaves disponiveis pode ser obtida pelo dataset retornado pela funcao set_dados na posicao keys. isto eh: print(tas[keys])
    '''
    mod = tas[chave_modelo].squeeze()
    
    if mod.coords['lon'][-1] > 180:
        mod = converte_longitude(mod)
        
    return mod


def converte_longitude(modelo):
    '''
    como alguns dos modelo vem com uma longitude de 0-360, utilizamos essa funcao para converter
    a escala para -180 a 180.
    '''
    modelo.coords['lon'] = (modelo.coords['lon'] + 180) % 360 - 180
    modelo = modelo.sortby(modelo.lon)
    return modelo

def limita_area_tempo(modelo, lat_init_fim, lon_init_fim, tempo_init, tempo_fim):
    '''
    @param modelo identifica o modelo que esta rodando
    @param lat_init latitude inicial, usada para limitar a area
    @param lat_fim latitude final, usada para limitar a area
    @param lon_init longitude inicial, usado no limite da area
    @param lon_fim longitude final, utilizado para limitar a area
    @param tempo_init, tempo_fim usado para limitar o intervalo de tempo, precisa ser str e no padrao AAAA-MM-DD
    '''

    model = modelo.sel(time=slice(tempo_init, tempo_fim))
    model_recorte =  model.sel(lon=slice(*lon_init_fim), lat=slice(*lat_init_fim),)
    return model_recorte


#em construcao, tem algum bug que será resolvido no próximo release
def formata_mapa(ax):
    '''
    funcao auxiliar utilizada para formatar o mapa de temperatura.
    '''
    ax.format(land = False, coast = True, innerborders = True, borders = True, labels = False, geogridlinewidth = 0, abcloc = 'ur', small = '25px', large = '25px')


#em construcao, tem algum bug que será resolvido no próximo release
def plota_mapa_temperatura(modelo, nomefig, coord_zonal, coord_merid, cor, var_indice, init, fim, passo, titulo):
    
    '''
    imprime um mapa de temperatura do modelo passado, filtrando o dataset para que a impressao leve em consideracao a partir dos anos de 1960 em diante.i
    atributo >> modelo - se refere ao modelo que deve ser usado pra gerar o mapa
    atributo >> nomefig - se refere ao nome da figura que sera salva em diretorio especifico.
    '''

    fig, ax = plot.subplots(axwidth = 8, tight = True, proj = 'robin', proj_kw = {'lon_0': 180},)
    formata_mapa(ax)
    map1 = ax.contourf(modelo[coord_zonal][0][0], modelo[coord_merid][0][0], modelo[var_indice] - 273.15, cmap = 'coolwarm', extend = 'both')
    ax.colorbar(map1, loc = 'b', shrink = 0.5, extendrect = True, labelsize = 18, ticklabelsize = 18, label = '%s ($\degree$C)' % titulo)
    fig.save(nomefig, dpi = 300)


def config_era5(uid = 103372,  url = "url: https://www.wcrp-climate.org/wgcm-cmip/wgcm-cmip6", key = 'key: '+ uid + ':eeacac2e-f54e-4c93-b664-7f754daab266'):
    '''
    prepara o ambiente para realizar o download dos dados de reanalise.
    esses dados serão posteriormente comparados com os do cmip6 para decidirmos estatisticamente o nivel de proximidade entre as duas entidades
    '''
    with open('/home/ubuntu/.cdsapirc', 'w') as f:
        f.write('\n'.join([url, key]))


def parametros_era5(mes1, mes2):
    '''
    essa funcao define os parametros que serao utilizados para fazer download dos dados de reanalise do ERA5.
    retorna um hash com os os dados para download.
    '''
    parametros = {
         'product_type': 'reanalysis',
         'format': 'netcdf',
         'variable': '2m_temperature',
         'year':
         [
             '1961', '1962', '1963',
             '1964', '1965', '1966',
             '1967', '1968', '1969',
             '1970', '1971', '1972',
             '1973', '1974', '1975',
             '1976', '1977', '1978',
             '1979', '1980', '1981',
             '1982', '1983', '1984',
             '1985', '1986', '1987',
             '1988', '1989', '1990',
             '1991', '1992', '1993',
             '1994', '1995', '1996',
             '1997', '1998', '1999',
             '2000', '2001', '2002',
             '2003', '2004', '2005',
             '2006', '2007', '2008',
             '2009', '2010', '2011',
             '2012', '2013', '2014',
         ],
         'month':
         [
             mes1, mes2,
         ],
         'day':
         [
             '01', '02', '03',
             '04', '05', '06',
             '07', '08', '09',
             '10', '11', '12',
             '13', '14', '15',
             '16', '17', '18',
             '19', '20', '21',
             '22', '23', '24',
             '25', '26', '27',
             '28', '29', '30',
             '31',
         ],
         'time':
         [
             '00:00', '01:00', '02:00',
             '03:00', '04:00', '05:00',
             '06:00', '07:00', '08:00',
             '09:00', '10:00', '11:00',
             '12:00', '13:00', '14:00',
             '15:00', '16:00', '17:00',
             '18:00', '19:00', '20:00',
             '21:00', '22:00', '23:00',
         ],
         'area':
             [
             5, -60, -35,
             -30,
             ],
     }
    return parametros


def download_era5(parametros, nomearq):
    '''
    Essa funcao deve rodar apenas 1x, pois ela faz download dos dados de reanalise do ERA5
    
    atributo >> parametros - define os parametros dos dados a serem baixados como por exemplo o mes, os dias, etc.
    '''

    print("Baixando dados de Reanalise do ERA5\nDados horários de 1961 - 2014 para cálculo dos índices do ETCCDI\nDisponível em: https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=form\n")
    dataset_era = 'reanalysis-era5-single-levels'
    c = cdsapi.Client()
    dir = ('/home/ubuntu/dados/%s' % nomearq)
    era = c.retrieve(dataset_era, parametros)
    era.download(dir)


def netframe_era5(dir):
    zipfile.ZipFile.extractall(dir)
    dset = xr.open_dataset('%s/era5.nc' % dir)
    netframe = xr.Dataset.to_dataframe(dset)
    return netframe


def calcula_indices(var_indice, calc_operation, var_input, slice_m, bimestre, model, in_file, repeat_time = False, control = 1, dir_completo):
    ''' Essa função gera os indices do ETCCDI para o conjunto de dados escolhido
    a funcao considera que estao sendo gerados indices bimestrais
    O argumento denominado 'var_indice' é algum dos indices gerados pelo ETCCDI, em caso de duvidas consultar documentação icclim
    O argumento denominado 'model' é apenas o nome do conjunto de dados para adicionar no caminho do arquivo
    de forma a salvar os dados nos respectivos diretórios, neste caso em especifico pode ser o ERA5 ou CMIP6
    FORMATO DE NOME DOS ARQUIVOS NETCDF: bimx.nc, onde 'x' representa o bimestre.
    eh recomendado o uso de aspas simples em todos os parametros (str)
    @param var_indice representa a variavel do indice que esta sendo analisada, por exemplo TXx, TNn...
    @param calc_operation representa o tipo de analise que esta sendo procurada, como por exemplo o calculo max ou min.
    @param var_input representa a variavel de entrada do netcdf original (cru), por exemplo t2m.
    @param slice_m indica o tipo de serie temporal analisada, por exemplo year, month, day...
    @param bimestre aponta o bimestre estudado uma vez que a api so permite o download bimestral para dados horarios do era5
    @param model indica o modelo para o qual esta sendo gerado o indice
    @param repeat_time (opcional) usado para realizar o calculo de varios arquivos recursivamente
    @param control enumera o bimestre
    @param in_file indica qual tipo de entrada para o icclim operar, pode ser um absolute_path ou uma OPeNDAP URL.
    '''

    print("Iniciando calculo do indice %s...\n" % var_indice)
    

    params = {'index_name': var_indice, 'calc_operation' : calc_operation, 'logical_operation' : 'gt', 'thresh' : 0 - 273.15}
    destiny_path = set_dir(dir_completo, model, var_indice, bimestre)
    icclim.index(user_index = params, in_files = in_file, var_name = var_input, slice_mode = slice_m, out_file = destiny_path)
    print("Calculo finalizado, o novo arquivo se encontra em %s\n" % destiny_path)
    
    if repeat_time:
        controle = control+1
        if control == 2:
            return calcula_indices(var_indice, calc_operation, var_input, slice_m, "bim%s" % controle, model, False, controle)
        return calcula_indices(var_indice, calc_operation, var_input, slice_m, "bim%s" % controle, model, True, controle)


def set_dir(dir_completo, modelo, var_indice, bim):
    '''
    retorna o path onde algum arquivo podera ser acessado/escrito.
    '''
    if modelo == "era5":
        return dir_completo + 'era5/%s_%s.nc' % (var_indice, bim)
    return dir_completo + 'cmip6/%s_%s.nc' % (var_indice, bim)


def interpolacao(settings_grid, model, path_destiny_name):
    '''
    faz o calculo de interpolacao por vizinhanca utilizando um modelo especifico
    @param settings_grid -> DEVE ser um arquivo txt contendo as configuracoes necessarias para realizar a interpolacao do modelo
    @param model -> DEVE ser um arquivo .nc (NETCDF4 ou HDF5) que carrega consigo os dados do modelo a ser interpolado. Tambem pode ser um absolute path indicando o local onde o arquivo em questao se encontra.
    @param path_destiny_name -> string que informa para a funcao a localizacao de destino do modelo interpolado.
    '''
    cdo = Cdo()
    cdo.remapdis(settings_grid, input = model, output = path_destiny_name)


def calc_pearson(model_era, model_cmip):
    '''
    busca calcular a correlação perfeita entre as duas variáveis
    '''
    mod_era = xr.open_dataset(model_era)
    mod_cmip = xr.open_dataset(model_cmip)
    return xr.corr(mod_era["TXx"], mod_cmip["TXx"])


def calc_erro_quadratico_medio(model_era, model_cmip):
    '''
    coeficiente estatístico que calcula a média de diferença quadrática entre a predição do modelo e o valor de destino.
    '''
    from sklearn.metrics import mean_squared_error
    return mean_squared_error(model_era, model_cmip)


def calc_erro_absoluto_medio(mod_era, mod_cmip):
    '''
    medida de erros entre observações pareadas que expressam o mesmo fenômeno. Exemplos de Y versus X incluem comparações de previsto versus observado, tempo subsequente versus tempo inicial e uma técnica de medição versus uma técnica alternativa de medição.
    '''
    from sklearn.metrics import mean_absolute_error
    return mean_absolute_error(mod_era, mod_cmip)


def drop_indexes(df):
    '''
    retorna o mesmo dataframe sem o index selecionado. entende-se por index uma coluna mais interna do dataframe.
    funcao auxiliar, utilizada no processamento do calculo dos erros quadratico e absoluto medio
    @param df, o dataframe que se deseja modificar
    @param index_name, a coluna interna que se deseja remover.
    '''
    df = df.reset_index(level = 'bounds')
    df = df.reset_index(level = 'time')
    df = df.reset_index(level = 'lat')
    df = df.reset_index(level = 'lon')
    df = df.drop('lon', axis = 1)
    df = df.drop('lat', axis = 1)
    df = df.drop('time', axis = 1)
    df = df.drop('bounds', axis = 1)
    return df
    
 
