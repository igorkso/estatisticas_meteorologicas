# Análise estatística de Dados Meteorológicos

Em meteorologia, sempre existiu uma imensidão de dados que precisam ser diariamente gerados, analisados, tratados e interpretados. Esta API fazer download e tratamento de modelos meteorológicos do CMIP6 - Coupled Model Intercomparison Project Phase 6 (https://pcmdi.llnl.gov/CMIP6/) e também do ERA -  European Centre for Medium-Range Weather Forecasts (https://www.ecmwf.int/en/about), que são dados de renálises meteorológicas. 

## Instalação e uso

Para fazer uso desta lib, você precisará escrever um main chamando as funções de download, interpolação, tratamento e cálculo estatísticos.

Para isso:

- Primeiro crie um ambiente virtual para instalar as bibliotecas necessárias:
```bash
sudo apt update
sudo apt install python3-venv
python3 -m venv
source venv/bin/activate
```

Use o [pip](https://pip.pypa.io/en/stable/) para instalar as bibliotecas, fazendo:

```python
pip install -r requirements.txt
```
