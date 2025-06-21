# Download SIA-BI para working directory
# Baseado no código do microdatasus

library(RCurl)
library(read.dbc)
library(arrow)
library(dplyr)
library(lubridate)

# Configurar pasta base no working directory
PASTA_BASE <- file.path(getwd(), "SIA-BI")

# Função para criar estrutura de pastas
criar_estrutura <- function(uf) {
  # Criar pasta principal se não existir
  if (!dir.exists(PASTA_BASE)) {
    dir.create(PASTA_BASE, recursive = TRUE)
    cat(paste("📁 Pasta principal criada:", PASTA_BASE, "\n"))
  }
  
  # Criar subpasta do estado
  pasta_estado <- file.path(PASTA_BASE, toupper(uf))
  if (!dir.exists(pasta_estado)) {
    dir.create(pasta_estado, recursive = TRUE)
    cat(paste("📁 Pasta do estado criada:", pasta_estado, "\n"))
  }
  
  return(pasta_estado)
}

# Função para verificar e baixar SIA-BI
baixar_siabi <- function(year_start,
                         month_start,
                         year_end,
                         month_end,
                         uf,
                         timeout = 240) {
  # Configurar timeout
  original_time_option <- getOption("timeout")
  on.exit(options(timeout = original_time_option))
  options(timeout = timeout)
  
  cat("🏥 === DOWNLOAD SIA-BI ===\n")
  cat(paste(
    "📅 Período:",
    paste0(month_start, "/", year_start),
    "a",
    paste0(month_end, "/", year_end),
    "\n"
  ))
  cat(paste("🏛️  Estado:", toupper(uf), "\n"))
  cat(paste("📂 Pasta destino:", PASTA_BASE, "\n\n"))
  
  # Criar datas
  date_start <- as.Date(paste0(
    year_start,
    "-",
    formatC(
      month_start,
      width = 2,
      format = "d",
      flag = "0"
    ),
    "-",
    "01"
  ))
  date_end <- as.Date(paste0(
    year_end,
    "-",
    formatC(
      month_end,
      width = 2,
      format = "d",
      flag = "0"
    ),
    "-",
    "01"
  ))
  
  # Verificar datas
  if (date_start > date_end)
    stop("❌ Data inicial deve ser menor que data final.")
  
  # Criar sequência de datas no formato yymm
  dates <- seq(date_start, date_end, by = "month")
  dates <- paste0(
    substr(lubridate::year(dates), 3, 4),
    formatC(
      lubridate::month(dates),
      width = 2,
      format = "d",
      flag = "0"
    )
  )
  
  # Estados válidos
  ufs <- c(
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO"
  )
  if (!toupper(uf) %in% ufs)
    stop(
      "❌ UF inválida! Use: AC, AL, AP, AM, BA, CE, DF, ES, GO, MA, MT, MS, MG, PA, PB, PR, PE, PI, RJ, RN, RS, RO, RR, SC, SP, SE, TO"
    )
  
  lista_uf <- toupper(uf)
  
  # URLs do DATASUS
  atual_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/"
  antigo_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/199407_200712/Dados/"
  
  cat("🔍 Verificando arquivos disponíveis no servidor...\n")
  
  # VERIFICAR ARQUIVOS DISPONÍVEIS (pasta atual)
  tryCatch({
    tmp <- unlist(strsplit(
      x = RCurl::getURL(
        url = atual_url,
        ftp.use.epsv = TRUE,
        dirlistonly = TRUE
      ),
      split = "\n"
    ))
    tmp <- tmp[grep("^BI", tmp)]
    tmp <- tmp[substr(x = tmp,
                      start = 3,
                      stop = 4) %in% lista_uf]
    avail_atual <- unique(substr(
      x = tmp,
      start = 5,
      stop = 8
    ))
    avail_atual <- gsub(pattern = "\\.",
                        replacement = "",
                        x = avail_atual)
  }, error = function(e) {
    avail_atual <<- c()
    cat("❌ Erro ao acessar pasta atual.\n")
  })
  
  # VERIFICAR ARQUIVOS DISPONÍVEIS (pasta antiga)
  tryCatch({
    tmp <- unlist(strsplit(
      x = RCurl::getURL(
        url = antigo_url,
        ftp.use.epsv = TRUE,
        dirlistonly = TRUE
      ),
      split = "\n"
    ))
    tmp <- tmp[grep("^BI", tmp)]
    tmp <- tmp[substr(x = tmp,
                      start = 3,
                      stop = 4) %in% lista_uf]
    avail_antigo <- unique(substr(
      x = tmp,
      start = 5,
      stop = 8
    ))
    avail_antigo <- gsub(pattern = "\\.",
                         replacement = "",
                         x = avail_antigo)
  }, error = function(e) {
    avail_antigo <<- c()
    cat("❌ Erro ao acessar pasta antiga.\n")
  })
  
  # Verificar se existem arquivos BI para o estado
  if (length(c(avail_atual, avail_antigo)) == 0) {
    cat(paste("❌ NENHUM arquivo BI encontrado para", toupper(uf), "!\n"))
    cat("💡 Arquivos BI podem não estar disponíveis para este estado.\n")
    cat("🔄 Recomendação: Use SIA-PA que tem melhor cobertura.\n")
    return(NULL)
  }
  
  # Verificar datas disponíveis
  if (!all(dates %in% c(avail_atual, avail_antigo))) {
    datas_nao_disponiveis <- dates[!dates %in% c(avail_atual, avail_antigo)]
    cat(paste(
      "⚠️  As seguintes datas não estão disponíveis (aamm):",
      paste0(datas_nao_disponiveis, collapse = ", "),
      "\n"
    ))
    cat("📋 Apenas as datas disponíveis serão baixadas.\n\n")
  }
  
  # Datas válidas
  valid_dates <- dates[dates %in% c(avail_atual, avail_antigo)]
  
  if (length(valid_dates) == 0) {
    cat("❌ Nenhuma data solicitada está disponível!\n")
    return(NULL)
  }
  
  # Mensagem sobre dados antigos
  if (any(valid_dates %in% avail_antigo)) {
    cat(
      paste(
        "📅 As seguintes datas são de pastas antigas (podem ter códigos incompatíveis):",
        paste0(valid_dates[valid_dates %in% avail_antigo], collapse = ", "),
        "\n\n"
      )
    )
  }
  
  # Criar lista de arquivos para download
  files_list_1 <- if (any(valid_dates %in% avail_antigo)) {
    paste0(antigo_url, "BI", as.vector(sapply(
      lista_uf, paste0, valid_dates[valid_dates %in% avail_antigo], ".dbc"
    )))
  }
  files_list_2 <- if (any(valid_dates %in% avail_atual)) {
    paste0(atual_url, "BI", as.vector(sapply(
      lista_uf, paste0, valid_dates[valid_dates %in% avail_atual], ".dbc"
    )))
  }
  files_list <- c(files_list_1, files_list_2)
  
  cat(paste("✅ Total de arquivos para download:", length(files_list), "\n"))
  
  # Criar estrutura de pastas
  pasta_estado <- criar_estrutura(uf)
  
  # Verificar conexão com a internet
  cat("🌐 Verificando conexão com a internet...\n")
  if (!curl::has_internet()) {
    stop("❌ Sem conexão com a internet!")
  }
  cat("✅ Conexão OK\n")
  
  # Verificar servidor DATASUS
  cat("🏥 Verificando servidor DATASUS...\n")
  if (!RCurl::url.exists("ftp.datasus.gov.br")) {
    cat("⚠️  Servidor DATASUS pode estar instável. Tentando mesmo assim...\n")
  } else {
    cat("✅ Servidor DATASUS OK\n")
  }
  
  cat("\n🚀 Iniciando downloads...\n\n")
  
  # Baixar e converter arquivos
  arquivos_baixados <- 0
  arquivos_erro <- 0
  arquivos_existentes <- 0
  
  for (file in files_list) {
    # Nome do arquivo
    nome_arquivo <- basename(file)
    
    # Extrair informações do nome do arquivo (BIGO0801.dbc = BI + GO + 08(ano) + 01(mês))
    estado_arquivo <- substr(nome_arquivo, 3, 4)  # GO
    ano_arquivo <- substr(nome_arquivo, 5, 6)     # 08
    mes_arquivo <- substr(nome_arquivo, 7, 8)     # 01
    
    # Reorganizar para ANO-MÊS (AAMM) - CORRIGIDO!
    ano_mes_correto <- paste0(ano_arquivo, mes_arquivo)  # 08 + 01 = 0801
    
    # Caminho do arquivo Parquet final
    arquivo_parquet <- file.path(pasta_estado,
                                 paste0(
                                   "bi",
                                   tolower(estado_arquivo),
                                   ano_mes_correto,
                                   ".parquet"
                                 ))
    
    # Verificar se arquivo já existe
    if (file.exists(arquivo_parquet)) {
      cat(paste("⏭️  Já existe:", basename(arquivo_parquet), "\n"))
      arquivos_existentes <- arquivos_existentes + 1
      next
    }
    
    cat(paste("📥 Baixando:", nome_arquivo, "..."))
    
    # Arquivo temporário
    temp <- tempfile()
    partial <- data.frame()
    
    # Tentar baixar arquivo
    tryCatch({
      utils::download.file(file,
                           temp,
                           mode = "wb",
                           method = "libcurl",
                           quiet = TRUE)
      partial <- read.dbc::read.dbc(temp, as.is = TRUE)
      file.remove(temp)
      
      # Salvar como Parquet
      if (nrow(partial) > 0) {
        write_parquet(partial, arquivo_parquet)
        arquivos_baixados <- arquivos_baixados + 1
        cat(" ✅ OK\n")
      } else {
        cat(" ⚠️  Arquivo vazio\n")
        arquivos_erro <- arquivos_erro + 1
      }
      
    }, error = function(cond) {
      cat(" ❌ ERRO\n")
      arquivos_erro <- arquivos_erro + 1
      if (file.exists(temp))
        file.remove(temp)
    })
    
    Sys.sleep(0.5)  # Pausa para não sobrecarregar servidor
  }
  
  # Relatório final
  cat("\n🎯 === RELATÓRIO FINAL ===\n")
  cat(paste("🏛️  Estado:", toupper(uf), "\n"))
  cat(paste("📥 Arquivos baixados:", arquivos_baixados, "\n"))
  cat(paste("⏭️  Arquivos já existentes:", arquivos_existentes, "\n"))
  cat(paste("❌ Arquivos com erro:", arquivos_erro, "\n"))
  cat(paste("📊 Taxa de sucesso:", round((
    arquivos_baixados / (length(files_list) - arquivos_existentes)
  ) * 100, 2), "%\n"))
  cat(paste("📂 Arquivos salvos em:", pasta_estado, "\n"))
  
  return(arquivos_baixados)
}

# Função simplificada para qualquer estado
baixar_siabi_estado <- function(estado,
                                ano_inicio = 2008,
                                ano_fim = 2025) {
  cat("🏥 === DOWNLOAD SIA-BI ===\n")
  cat(paste("🏛️  Estado:", toupper(estado), "\n"))
  cat(paste("📅 Período:", ano_inicio, "a", ano_fim, "\n"))
  cat(paste("📂 Working Directory:", getwd(), "\n"))
  cat(paste("📂 Destino:", PASTA_BASE, "\n\n"))
  
  # Verificar se estado é válido
  estados_validos <- c(
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO"
  )
  
  if (!toupper(estado) %in% estados_validos) {
    stop(
      "❌ Estado inválido! Use: AC, AL, AP, AM, BA, CE, DF, ES, GO, MA, MT, MS, MG, PA, PB, PR, PE, PI, RJ, RN, RS, RO, RR, SC, SP, SE, TO"
    )
  }
  
  resultado <- baixar_siabi(
    year_start = ano_inicio,
    month_start = 1,
    year_end = ano_fim,
    month_end = 12,
    uf = toupper(estado)
  )
  
  return(resultado)
}

# Função para verificar disponibilidade
verificar_siabi_disponibilidade <- function(uf) {
  cat(paste(
    "🔍 Verificando disponibilidade SIA-BI para",
    toupper(uf),
    "\n"
  ))
  cat(paste("📂 Working Directory:", getwd(), "\n"))
  cat(paste("📂 Pasta destino:", PASTA_BASE, "\n\n"))
  
  atual_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/"
  antigo_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/199407_200712/Dados/"
  
  lista_uf <- toupper(uf)
  
  # Verificar pasta atual
  tryCatch({
    tmp <- unlist(strsplit(
      x = RCurl::getURL(
        url = atual_url,
        ftp.use.epsv = TRUE,
        dirlistonly = TRUE
      ),
      split = "\n"
    ))
    tmp <- tmp[grep("^BI", tmp)]
    tmp <- tmp[substr(x = tmp,
                      start = 3,
                      stop = 4) %in% lista_uf]
    avail_atual <- unique(substr(
      x = tmp,
      start = 5,
      stop = 8
    ))
    cat(paste(
      "📂 Pasta atual (2008+):",
      length(avail_atual),
      "arquivos encontrados\n"
    ))
    if (length(avail_atual) > 0) {
      cat(paste("   Exemplo:", paste0("BI", uf, avail_atual[1], ".dbc"), "\n"))
    }
  }, error = function(e) {
    cat("❌ Erro ao acessar pasta atual\n")
    avail_atual <<- c()
  })
  
  # Verificar pasta antiga
  tryCatch({
    tmp <- unlist(strsplit(
      x = RCurl::getURL(
        url = antigo_url,
        ftp.use.epsv = TRUE,
        dirlistonly = TRUE
      ),
      split = "\n"
    ))
    tmp <- tmp[grep("^BI", tmp)]
    tmp <- tmp[substr(x = tmp,
                      start = 3,
                      stop = 4) %in% lista_uf]
    avail_antigo <- unique(substr(
      x = tmp,
      start = 5,
      stop = 8
    ))
    cat(paste(
      "📂 Pasta antiga (1994-2007):",
      length(avail_antigo),
      "arquivos encontrados\n"
    ))
    if (length(avail_antigo) > 0) {
      cat(paste("   Exemplo:", paste0("BI", uf, avail_antigo[1], ".dbc"), "\n"))
    }
  }, error = function(e) {
    cat("❌ Erro ao acessar pasta antiga\n")
    avail_antigo <<- c()
  })
  
  total <- length(c(avail_atual, avail_antigo))
  cat(paste(
    "\n📊 Total de arquivos SIA-BI disponíveis para",
    toupper(uf),
    ":",
    total,
    "\n"
  ))
  
  if (total == 0) {
    cat("\n❌ NENHUM arquivo SIA-BI disponível para este estado!\n")
    cat("💡 Experimente outros tipos: SIA-PA, SIA-AM, SIA-AD\n")
  } else {
    cat("\n✅ Arquivos SIA-BI estão disponíveis!\n")
    cat(paste(
      "🚀 Use: baixar_siabi_estado('",
      tolower(uf),
      "')\n",
      sep = ""
    ))
  }
  
  return(total > 0)
}

# ================================
# EXEMPLOS DE USO:
# ================================

cat("🏥 === DOWNLOADER SIA-BI ===\n")
cat(paste("📂 Working Directory:", getwd(), "\n"))
cat(paste("📂 Pasta destino:", PASTA_BASE, "\n\n"))
cat("📋 Funções disponíveis:\n")
cat("1. verificar_siabi_disponibilidade('GO') - Verificar disponibilidade\n")
cat("2. baixar_siabi_estado('GO') - Baixar histórico completo\n")
cat("3. baixar_siabi_estado('GO', 2020, 2024) - Período específico\n\n")

cat("🚀 EXEMPLO PARA GOIÁS:\n")
cat("verificar_siabi_disponibilidade('GO')\n")
cat("baixar_siabi_estado('GO')  # Todo o histórico\n\n")

cat("🌎 OUTROS ESTADOS:\n")
cat("baixar_siabi_estado('SP')  # São Paulo\n")
cat("baixar_siabi_estado('RJ')  # Rio de Janeiro\n")
cat("baixar_siabi_estado('MG')  # Minas Gerais\n")
cat("\n💡 FORMATO DOS ARQUIVOS: bi[estado][ano][mês].parquet\n")
cat("Exemplo: bigo0801.parquet = BI Goiás Janeiro/2008\n")


# ==============================
# Dados do Norte do Brasil
# ==============================
verificar_siabi_disponibilidade('AM')  # Amazonas
verificar_siabi_disponibilidade('AC')  # Acre
verificar_siabi_disponibilidade('AP')  # Amapá
verificar_siabi_disponibilidade('PA')  # Pará
verificar_siabi_disponibilidade('RO')  # Rondônia
verificar_siabi_disponibilidade('RR')  # Roraima
verificar_siabi_disponibilidade('TO')  # Tocantins

baixar_siabi_estado('AM')  # Baixar todo o histórico do Amazonas
baixar_siabi_estado('AC')  # Baixar todo o histórico do Acre
baixar_siabi_estado('AP')  # Baixar todo o histórico do Amapá
baixar_siabi_estado('PA')  # Baixar todo o histórico do Pará
baixar_siabi_estado('RO')  # Baixar todo o histórico de Rondônia
baixar_siabi_estado('RR')  # Baixar todo o histórico de Roraima
baixar_siabi_estado('TO')  # Baixar todo o histórico de Tocantins

# ==============================
# Dados do Nordeste do Brasil 
# ==============================
verificar_siabi_disponibilidade('BA')  # Bahia
verificar_siabi_disponibilidade('CE')  # Ceará
verificar_siabi_disponibilidade('MA')  # Maranhão
verificar_siabi_disponibilidade('PB')  # Paraíba
verificar_siabi_disponibilidade('PE')  # Pernambuco
verificar_siabi_disponibilidade('PI')  # Piauí
verificar_siabi_disponibilidade('RN')  # Rio Grande do Norte
verificar_siabi_disponibilidade('SE')  # Sergipe
verificar_siabi_disponibilidade('AL')  # Alagoas

baixar_siabi_estado('BA')  # Baixar todo o histórico da Bahia
baixar_siabi_estado('CE')  # Baixar todo o histórico do Ceará
baixar_siabi_estado('MA')  # Baixar todo o histórico do Maranhão
baixar_siabi_estado('PB')  # Baixar todo o histórico da Paraíba
baixar_siabi_estado('PE')  # Baixar todo o histórico de Pernambuco
baixar_siabi_estado('PI')  # Baixar todo o histórico do Piauí
baixar_siabi_estado('RN')  # Baixar todo o histórico do Rio Grande do Norte
baixar_siabi_estado('SE')  # Baixar todo o histórico de Sergipe
baixar_siabi_estado('AL')  # Baixar todo o histórico de Alagoas

# ==============================
# Dados do Centro-Oeste do Brasil 
# ==============================
verificar_siabi_disponibilidade('DF')  # Distrito Federal
verificar_siabi_disponibilidade('GO')  # Goiás
verificar_siabi_disponibilidade('MS')  # Mato Grosso do Sul
verificar_siabi_disponibilidade('MT')  # Mato Grosso

baixar_siabi_estado('DF')  # Baixar todo o histórico do Distrito Federal
baixar_siabi_estado('GO')  # Baixar todo o histórico de Goiás
baixar_siabi_estado('MS')  # Baixar todo o histórico de Mato Grosso do Sul
baixar_siabi_estado('MT')  # Baixar todo o histórico de Mato Grosso

# ==============================
# Dados do Sudeste do Brasil
# ==============================
verificar_siabi_disponibilidade('ES')  # Espírito Santo
verificar_siabi_disponibilidade('MG')  # Minas Gerais
verificar_siabi_disponibilidade('RJ')  # Rio de Janeiro
verificar_siabi_disponibilidade('SP')  # São Paulo

baixar_siabi_estado('ES')  # Baixar todo o histórico do Espírito Santo
baixar_siabi_estado('MG')  # Baixar todo o histórico de Minas Gerais
baixar_siabi_estado('RJ')  # Baixar todo o histórico do Rio de Janeiro
baixar_siabi_estado('SP')  # Baixar todo o histórico de São Paulo

# ==============================
# Dados do Sul do Brasil
# ==============================
verificar_siabi_disponibilidade('PR')  # Paraná
verificar_siabi_disponibilidade('RS')  # Rio Grande do Sul
verificar_siabi_disponibilidade('SC')  # Santa Catarina

baixar_siabi_estado('PR')  # Baixar todo o histórico do Paraná
baixar_siabi_estado('RS')  # Baixar todo o histórico do Rio Grande do Sul
baixar_siabi_estado('SC')  # Baixar todo o histórico de Santa Catarina

# ==============================