## 🏨 Download Automático SIH-RD — Sistema de Informações Hospitalares Reduzido (DATASUS | 2025)

### 🎯 Objetivo do Projeto

Este projeto automatiza o download e processamento de dados do SIH-RD (Sistema de Informações Hospitalares - AIH Reduzida) do DATASUS, facilitando o acesso a informações de internações hospitalares do SUS para pesquisa e análise epidemiológica.

**A abordagem envolve:**

* Download automatizado de dados hospitalares do FTP DATASUS
* Conversão de arquivos `.dbc` para formato `.parquet` otimizado
* Organização por estado e cronologia temporal
* Verificação de integridade e relatórios detalhados
* Processamento de dados de internação em larga escala

---

### 📂 Estrutura dos Dados

A base de dados **SIH-RD** cobre internações hospitalares entre 1992 e 2025 (conforme disponibilidade) e inclui:

* **Internações Hospitalares**: Dados da AIH (Autorização de Internação Hospitalar) em formato reduzido
* **Diagnósticos**: CID-10 principal e secundários (até 9 diagnósticos)
* **Procedimentos**: Códigos de procedimentos realizados durante internação
* **Tempo de Permanência**: Datas de admissão e alta hospitalar
* **Demografia**: Idade, sexo, raça/cor, escolaridade do paciente
* **Geolocalização**: Município de residência e internação
* **Desfechos**: Motivo da saída (alta, óbito, transferência)
* **Financeiro**: Valores aprovados pelo SUS

**Estrutura de arquivos gerados:**
```
[Working Directory]/
└── SIH-RD/
    ├── GO/
    │   ├── rdgo0801.parquet  # Goiás Janeiro/2008
    │   ├── rdgo0802.parquet  # Goiás Fevereiro/2008
    │   └── ...
    ├── SP/
    │   ├── rdsp0801.parquet  # São Paulo Janeiro/2008
    │   └── ...
    └── [outros estados...]
```

**Fontes utilizadas:**
* FTP DATASUS: `ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/`
* Pasta atual (2008+): `200801_/Dados/`
* Pasta antiga (1992-2007): `199201_200712/Dados/`

---

### 🧼 Processamento e Padronização

O script `SIH-RD Download` realiza as seguintes etapas:

* **Varredura de Disponibilidade**: Identifica arquivos RD disponíveis por estado/período
* **Download Inteligente**: Evita redundância verificando arquivos existentes
* **Conversão Otimizada**: Transforma `.dbc` em `.parquet` para análise eficiente
* **Validação Rigorosa**: Detecta arquivos corrompidos ou vazios
* **Nomenclatura Padronizada**: `rd[estado][ano][mês].parquet`
* **Relatórios Completos**:
  * Total de arquivos processados
  * Arquivos pré-existentes (saltados)
  * Falhas de download com detalhes
  * Taxa de sucesso percentual
* **Throttling**: Pausas estratégicas para preservar servidor DATASUS

---

### 📊 Funções Principais

#### **Verificação de Disponibilidade**
```r
verificar_sihrd_disponibilidade('MG')
# Exibe quantos arquivos RD estão disponíveis para Minas Gerais
```

#### **Download por Estado**
```r
# Download histórico completo (1992-2025)
baixar_sihrd_estado('MG')

# Download período específico
baixar_sihrd_estado('MG', ano_inicio = 2018, ano_fim = 2023)
```

#### **Download Personalizado**
```r
# Controle granular de período
baixar_sihrd(
  year_start = 2020, month_start = 6,
  year_end = 2023, month_end = 12,
  uf = 'RJ', timeout = 300
)
```

---

### 🔍 Análise de Cobertura por Região

#### **Estados com Excelente Cobertura SIH-RD:**
* **Sudeste**: SP, MG, RJ, ES — Histórico completo desde 1992
* **Sul**: PR, RS, SC — Dados consistentes e abrangentes
* **Nordeste**: BA, PE, CE, PB — Boa cobertura temporal
* **Centro-Oeste**: GO, MT, MS, DF — Dados robustos

#### **Características dos Dados:**
* **Volume**: Estados populosos geram arquivos grandes (SP >10GB/ano)
* **Qualidade**: SIH-RD tem melhor cobertura que SIA-BI para a maioria dos estados
* **Período**: Dados hospitalares geralmente mais estáveis que ambulatoriais
* **Completude**: Informações obrigatórias para reembolso SUS garantem qualidade

---

### 💻 Tecnologias Utilizadas

* **Linguagem**: `R`
* **Principais Pacotes**:
  * Conexão: `RCurl`, `curl` para acesso FTP
  * Processamento: `read.dbc`, `arrow` para conversão de formatos
  * Manipulação: `dplyr`, `lubridate` para transformação de dados
  * Sistema: Funções R base para gerenciamento de arquivos e diretórios

---

### ⚙️ Requisitos do Sistema

* **R** versão 4.0+ com pacotes atualizados
* **Pacotes obrigatórios**: `RCurl`, `read.dbc`, `arrow`, `dplyr`, `lubridate`, `curl`
* **Espaço em disco**: ~100-200GB para dados completos (todos os estados/anos)
* **Conexão**: Internet estável de alta velocidade (downloads extensos)
* **RAM**: Mínimo 16GB recomendado (arquivos SIH-RD são maiores que SIA)
* **CPU**: Processador multi-core para conversão eficiente

---

### ▶️ Como Reproduzir

1. **Instalação de Dependências**
   ```r
   # Pacotes essenciais
   install.packages(c("RCurl", "read.dbc", "arrow", "dplyr", "lubridate", "curl"))
   
   # Verificar instalação read.dbc (crítico para .dbc)
   library(read.dbc)
   ```

2. **Configuração do Ambiente**
   ```r
   # Definir diretório de trabalho
   setwd("C:/projetos/dados_hospitalar")  # Ajuste conforme necessário
   
   # Verificar espaço disponível
   dir.create("SIH-RD", showWarnings = FALSE)
   ```

3. **Carregamento do Script**
   ```r
   source("sih_rd_download.R")
   ```

4. **Teste Inicial** (recomendado)
   ```r
   # Verificar disponibilidade
   verificar_sihrd_disponibilidade('GO')
   
   # Download teste (apenas 2023)
   baixar_sihrd_estado('GO', ano_inicio = 2023, ano_fim = 2023)
   ```

5. **Download Regional** (abordagem gradual)
   ```r
   # Região Centro-Oeste (menor volume)
   centro_oeste <- c("GO", "MT", "MS", "DF")
   for(estado in centro_oeste) {
     baixar_sihrd_estado(estado)
     Sys.sleep(30)  # Pausa entre estados
   }
   ```

6. **Download Nacional** (uso avançado)
   ```r
   # ATENÇÃO: Processo pode durar vários dias!
   todos_estados <- c("AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", 
                      "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", 
                      "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO")
   
   for(estado in todos_estados) {
     cat(paste("Processando", estado, "...\n"))
     baixar_sihrd_estado(estado)
     Sys.sleep(60)  # Pausa entre estados
   }
   ```

---

### 📋 Formato e Conteúdo dos Dados

**Exemplo de arquivo SIH-RD:**
* **Nome**: `rdsp2301.parquet` (RD + São Paulo + Janeiro/2023)
* **Formato**: Apache Parquet (compressão eficiente, consulta rápida)
* **Tamanho típico**: 500MB - 2GB por arquivo (varia por estado/período)

**Variáveis principais incluem:**
* **Identificação**: Número AIH, estabelecimento, profissional
* **Demografia**: Idade, sexo, município residência
* **Clínica**: CID principal, CIDs secundários, procedimento principal
* **Temporal**: Data internação, data saída, dias permanência
* **Desfecho**: Motivo saída (1=alta, 2=transferência, 3=óbito)
* **Financeiro**: Valor total, valor UTI, valor procedimentos

---

### ⚠️ Considerações Críticas

* **Volume Massivo**: Estados como SP e MG geram dezenas de GB por ano
* **Tempo Extenso**: Download completo pode levar 3-7 dias dependendo da conexão
* **Estabilidade DATASUS**: Servidor pode ficar indisponível em horários de pico
* **Evolução Temporal**: Estrutura de variáveis pode mudar entre períodos históricos
* **Responsabilidade**: Dados contêm informações sensíveis (anonimizadas) de saúde
* **Uso Ético**: Respeitar diretrizes de pesquisa em saúde pública

---

### 🎯 Aplicações Típicas

* **Epidemiologia**: Análise de padrões de morbidade e mortalidade
* **Gestão Hospitalar**: Estudos de tempo de permanência e desfechos
* **Economia da Saúde**: Análise de custos e eficiência hospitalar
* **Pesquisa Clínica**: Estudos de coorte e séries temporais
* **Políticas Públicas**: Avaliação de impacto de intervenções em saúde

---

### 🔗 Recursos Adicionais

* **Portal DATASUS**: https://datasus.saude.gov.br/
* **Documentação SIH**: https://datasus.saude.gov.br/acesso-a-informacao/sih-sistema-de-informacoes-hospitalares/
* **SIGTAP** (Tabela de Procedimentos): http://sigtap.datasus.gov.br/
* **CID-10**: https://icd.who.int/browse10/2019/en
* **Manual AIH**: Disponível no portal DATASUS

---

Desenvolvido para pesquisa avançada em informações hospitalares do Sistema Único de Saúde.
**Versão**: 2025.1 | **Última atualização**: Janeiro 2025