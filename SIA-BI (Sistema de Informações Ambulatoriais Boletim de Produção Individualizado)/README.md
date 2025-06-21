## 🏥 Download Automático SIA-BI — Sistema de Informações Ambulatoriais (DATASUS | 2025)

### 🎯 Objetivo do Projeto

Este projeto automatiza o download e processamento de dados do SIA-BI (Sistema de Informações Ambulatoriais - Boletim de Produção Individualizado) do DATASUS, facilitando o acesso a informações ambulatoriais do SUS para pesquisa e análise.

**A abordagem envolve:**

* Download automatizado do FTP do DATASUS
* Conversão de arquivos `.dbc` para formato `.parquet` otimizado
* Organização por estado e período temporal
* Verificação de disponibilidade e integridade dos dados
* Relatórios detalhados de progresso e erros

---

### 📂 Estrutura dos Dados

A base de dados **SIA-BI** cobre procedimentos ambulatoriais entre 1994 e 2025 (conforme disponibilidade) e inclui:

* **Procedimentos Ambulatoriais**: Consultas médicas, exames diagnósticos, terapias
* **Dados Individualizados**: Informações por paciente (anonimizadas)
* **Classificações**: CID-10, procedimentos SUS, especialidades médicas
* **Localização**: Município de residência e atendimento
* **Temporal**: Data de realização do procedimento
* **Financeiro**: Valores aprovados e pagos pelo SUS

**Estrutura de arquivos gerados:**
```
[Working Directory]/
└── SIA-BI/
    ├── GO/
    │   ├── bigo0801.parquet  # Goiás Janeiro/2008
    │   ├── bigo0802.parquet  # Goiás Fevereiro/2008
    │   └── ...
    ├── SP/
    │   ├── bisp0801.parquet  # São Paulo Janeiro/2008
    │   └── ...
    └── [outros estados...]
```

**Fontes utilizadas:**
* FTP DATASUS: `ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/`
* Pasta atual (2008+): `200801_/Dados/`
* Pasta antiga (1994-2007): `199407_200712/Dados/`

---

### 🧼 Processamento e Padronização

O script `SIA-BI Download` realiza as seguintes etapas:

* **Verificação de Disponibilidade**: Consulta FTP para arquivos existentes por estado
* **Download Inteligente**: Evita redownload de arquivos já processados
* **Conversão Automática**: Transforma `.dbc` em `.parquet` para melhor performance
* **Validação de Dados**: Verifica arquivos vazios ou corrompidos
* **Organização Temporal**: Nomenclatura padronizada `bi[estado][ano][mês].parquet`
* **Relatórios Detalhados**: 
  * Arquivos baixados com sucesso
  * Arquivos já existentes (skip)
  * Arquivos com erro
  * Taxa de sucesso por estado
* **Controle de Carga**: Pausa entre downloads para não sobrecarregar servidor

---

### 📊 Funções Principais

#### **Verificação de Disponibilidade**
```r
verificar_siabi_disponibilidade('SP')
# Mostra quantos arquivos estão disponíveis para São Paulo
```

#### **Download por Estado**
```r
# Download histórico completo (1994-2025)
baixar_siabi_estado('SP')

# Download período específico
baixar_siabi_estado('SP', ano_inicio = 2020, ano_fim = 2024)
```

#### **Download Personalizado**
```r
# Controle total de período
baixar_siabi(
  year_start = 2020, month_start = 1,
  year_end = 2024, month_end = 12,
  uf = 'SP', timeout = 300
)
```

---

### 🔍 Análise de Cobertura por Região

#### **Estados com Melhor Cobertura SIA-BI:**
* **Sudeste**: SP, RJ, MG, ES — Cobertura completa
* **Sul**: PR, RS, SC — Dados consistentes
* **Nordeste**: BA, PE, CE — Boa disponibilidade
* **Centro-Oeste**: GO, DF — Cobertura adequada

#### **Estados com Limitações:**
* Alguns estados podem ter arquivos BI indisponíveis em certos períodos
* Recomendação: verificar disponibilidade antes do download massivo
* Alternativa: usar SIA-PA (Procedimentos Ambulatoriais) com melhor cobertura

---

### 💻 Tecnologias Utilizadas

* **Linguagem**: `R`
* **Principais Pacotes**:
  * Manipulação: `dplyr`, `lubridate`
  * Download: `RCurl`, `curl`, `utils`
  * Processamento: `read.dbc`, `arrow`
  * Sistema: Funções nativas do R para gerenciamento de arquivos

---

### ⚙️ Requisitos do Sistema

* **R** versão 4.0+
* **Pacotes obrigatórios**: `RCurl`, `read.dbc`, `arrow`, `dplyr`, `lubridate`
* **Espaço em disco**: ~50-100GB para dados completos (todos os estados)
* **Conexão**: Internet estável (downloads podem durar horas)
* **RAM**: Mínimo 8GB recomendado para processamento

---

### ▶️ Como Reproduzir

1. **Instale os Pacotes Necessários**
   ```r
   install.packages(c("RCurl", "read.dbc", "arrow", "dplyr", "lubridate", "curl"))
   ```

2. **Configure o Working Directory**
   ```r
   setwd("C:/meu_projeto/dados_sus")  # Substitua pelo seu caminho
   ```

3. **Execute o Script SIA-BI**
   ```r
   source("sia_bi_download.R")
   ```

4. **Teste com um Estado**
   ```r
   # Verificar disponibilidade
   verificar_siabi_disponibilidade('GO')
   
   # Download teste (só 2024)
   baixar_siabi_estado('GO', ano_inicio = 2024, ano_fim = 2024)
   ```

5. **Download Completo** (opcional)
   ```r
   # Todos os estados do Brasil (CUIDADO: pode demorar dias!)
   estados <- c("AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", 
                "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", 
                "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO")
   
   for(estado in estados) {
     baixar_siabi_estado(estado)
   }
   ```

---

### 📋 Formato dos Dados

**Exemplo de arquivo SIA-BI:**
* **Nome**: `bigo0801.parquet` (BI + Goiás + Janeiro/2008)
* **Formato**: Apache Parquet (comprimido e otimizado)
* **Conteúdo**: Procedimentos ambulatoriais individualizados
* **Variáveis típicas**: 
  * Código do procedimento
  * CID-10 principal e secundário
  * Município de residência/atendimento
  * Data de realização
  * Valores financeiros
  * Dados demográficos (idade, sexo)

---

### ⚠️ Considerações Importantes

* **Volume de Dados**: Estados grandes (SP, MG) geram arquivos de vários GB
* **Tempo de Download**: Processo pode levar várias horas ou dias
* **Estabilidade**: Servidor DATASUS pode ficar instável ocasionalmente
* **Compatibilidade**: Códigos podem mudar entre períodos (dados antigos vs atuais)
* **Uso Responsável**: Evite downloads simultâneos excessivos para não sobrecarregar o servidor

---

### 🔗 Links Úteis

* **DATASUS**: https://datasus.saude.gov.br/
* **Documentação SIA**: https://datasus.saude.gov.br/acesso-a-informacao/sia-sistema-de-informacoes-ambulatoriais-do-sus/
* **Tabelas SUS**: http://sigtap.datasus.gov.br/

---

Desenvolvido para facilitar o acesso a dados de saúde pública brasileira.
**Versão**: 2025.1