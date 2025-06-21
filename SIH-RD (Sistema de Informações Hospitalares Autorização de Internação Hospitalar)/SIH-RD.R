# Download SIH-RD para working directory
# Sistema de Informações Hospitalares - AIH Reduzida

library(RCurl)
library(read.dbc)
library(arrow)
library(dplyr)
library(lubridate)

# Configurar pasta base no working directory
PASTA_BASE <- file.path(getwd(), "SIH-RD")

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

# Função para verificar e baixar SIH-RD
baixar_sihrd <- function(year_start,
                         month_start,
                         year_end,
                         month_end,
                         uf,
                         timeout = 240) {
  # Configurar timeout
  original_time_option <- getOption("timeout")
  on.exit(options(timeout = original_time_option))
  options(timeout = timeout)
  
  cat("🏨 === DOWNLOAD SIH-RD ===\n")
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
  
  # URLs do DATASUS - SIH (diferentes do SIA)
  atual_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/"
  antigo_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/199201_200712/Dados/"
  
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
    tmp <- tmp[grep("^RD", tmp)]  # Prefixo RD para SIH-RD
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
    tmp <- tmp[grep("^RD", tmp)]  # Prefixo RD para SIH-RD
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
  
  # Verificar se existem arquivos RD para o estado
  if (length(c(avail_atual, avail_antigo)) == 0) {
    cat(paste("❌ NENHUM arquivo SIH-RD encontrado para", toupper(uf), "!\n"))
    cat("💡 Arquivos SIH-RD podem não estar disponíveis para este estado.\n")
    cat("🔄 Recomendação: Verifique outros tipos de dados hospitalares.\n")
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
    paste0(antigo_url, "RD", as.vector(sapply(
      lista_uf, paste0, valid_dates[valid_dates %in% avail_antigo], ".dbc"
    )))
  }
  files_list_2 <- if (any(valid_dates %in% avail_atual)) {
    paste0(atual_url, "RD", as.vector(sapply(
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
    
    # Extrair informações do nome do arquivo (RDGO0801.dbc = RD + GO + 08(ano) + 01(mês))
    estado_arquivo <- substr(nome_arquivo, 3, 4)  # GO
    ano_arquivo <- substr(nome_arquivo, 5, 6)     # 08
    mes_arquivo <- substr(nome_arquivo, 7, 8)     # 01
    
    # Reorganizar para ANO-MÊS (AAMM) - CORRIGIDO!
    ano_mes_correto <- paste0(ano_arquivo, mes_arquivo)  # 08 + 01 = 0801
    
    # Caminho do arquivo Parquet final
    arquivo_parquet <- file.path(pasta_estado,
                                 paste0(
                                   "rd",
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
baixar_sihrd_estado <- function(estado,
                                ano_inicio = 2008,
                                ano_fim = 2025) {
  cat("🏨 === DOWNLOAD SIH-RD ===\n")
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
  
  resultado <- baixar_sihrd(
    year_start = ano_inicio,
    month_start = 1,
    year_end = ano_fim,
    month_end = 12,
    uf = toupper(estado)
  )
  
  return(resultado)
}

# Função para verificar disponibilidade
verificar_sihrd_disponibilidade <- function(uf) {
  cat(paste(
    "🔍 Verificando disponibilidade SIH-RD para",
    toupper(uf),
    "\n"
  ))
  cat(paste("📂 Working Directory:", getwd(), "\n"))
  cat(paste("📂 Pasta destino:", PASTA_BASE, "\n\n"))
  
  atual_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/"
  antigo_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/199201_200712/Dados/"
  
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
    tmp <- tmp[grep("^RD", tmp)]
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
      cat(paste("   Exemplo:", paste0("RD", uf, avail_atual[1], ".dbc"), "\n"))
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
    tmp <- tmp[grep("^RD", tmp)]
    tmp <- tmp[substr(x = tmp,
                      start = 3,
                      stop = 4) %in% lista_uf]
    avail_antigo <- unique(substr(
      x = tmp,
      start = 5,
      stop = 8
    ))
    cat(paste(
      "📂 Pasta antiga (1992-2007):",
      length(avail_antigo),
      "arquivos encontrados\n"
    ))
    if (length(avail_antigo) > 0) {
      cat(paste("   Exemplo:", paste0("RD", uf, avail_antigo[1], ".dbc"), "\n"))
    }
  }, error = function(e) {
    cat("❌ Erro ao acessar pasta antiga\n")
    avail_antigo <<- c()
  })
  
  total <- length(c(avail_atual, avail_antigo))
  cat(paste(
    "\n📊 Total de arquivos SIH-RD disponíveis para",
    toupper(uf),
    ":",
    total,
    "\n"
  ))
  
  if (total == 0) {
    cat("\n❌ NENHUM arquivo SIH-RD disponível para este estado!\n")
    cat("💡 Experimente outros tipos: SIH-SP, SIH-ER, etc.\n")
  } else {
    cat("\n✅ Arquivos SIH-RD estão disponíveis!\n")
    cat(paste(
      "🚀 Use: baixar_sihrd_estado('",
      tolower(uf),
      "')\n",
      sep = ""
    ))
  }
  
  return(total > 0)
}

cat("🏨 === DOWNLOAD COMPLETO SIH-RD BRASIL ===\n")
cat("✅ VERSÃO CORRIGIDA: Formato ano-mês (AAMM)\n")
cat("📊 Dados: Sistema de Informações Hospitalares - AIH Reduzida\n")
cat("📅 Período: Histórico completo disponível (1992-2025)\n")
cat("🏛️  Estados: Todos os 27 estados + DF\n")
cat(paste("📂 Working Directory:", getwd(), "\n"))
cat(paste("📂 Destino:", PASTA_BASE, "\n\n"))

cat("⚠️  IMPORTANTE:\n")
cat("- Certifique-se de usar o código SIH-RD CORRIGIDO\n")
cat("- Processo pode demorar várias horas ou dias\n")
cat("- Requer MUITO espaço em disco (100GB+)\n")
cat("- Conexão estável com internet\n")
cat("- Arquivos de internação são maiores que ambulatoriais\n\n")

cat("📋 DADOS QUE SERÃO BAIXADOS:\n")
cat("🏨 SIH-RD = Internações hospitalares (AIH Reduzida)\n")
cat("📊 Conteúdo: Diagnósticos, procedimentos, tempo internação\n")
cat("📁 Formato final: rdgo0801.parquet = Goiás Janeiro/2008\n\n")

# ==============================
# REGIÃO NORTE (7 ESTADOS)
# ==============================
cat("🌳 === REGIÃO NORTE ===\n")
cat("Estados: AM, AC, AP, PA, RO, RR, TO\n\n")

# Verificar disponibilidade - NORTE
cat("🔍 Verificando disponibilidade SIH-RD - NORTE:\n")
verificar_sihrd_disponibilidade('AM')  # Amazonas
verificar_sihrd_disponibilidade('AC')  # Acre
verificar_sihrd_disponibilidade('AP')  # Amapá
verificar_sihrd_disponibilidade('PA')  # Pará
verificar_sihrd_disponibilidade('RO')  # Rondônia
verificar_sihrd_disponibilidade('RR')  # Roraima
verificar_sihrd_disponibilidade('TO')  # Tocantins

cat("\n🚀 Iniciando downloads SIH-RD - REGIÃO NORTE:\n")
# Download histórico completo - NORTE
baixar_sihrd_estado('AM')  # Amazonas - Histórico completo
baixar_sihrd_estado('AC')  # Acre - Histórico completo
baixar_sihrd_estado('AP')  # Amapá - Histórico completo
baixar_sihrd_estado('PA')  # Pará - Histórico completo
baixar_sihrd_estado('RO')  # Rondônia - Histórico completo
baixar_sihrd_estado('RR')  # Roraima - Histórico completo
baixar_sihrd_estado('TO')  # Tocantins - Histórico completo

# ==============================
# REGIÃO NORDESTE (9 ESTADOS)
# ==============================
cat("\n🏖️ === REGIÃO NORDESTE ===\n")
cat("Estados: MA, PI, CE, RN, PB, PE, AL, SE, BA\n\n")

# Verificar disponibilidade - NORDESTE
cat("🔍 Verificando disponibilidade SIH-RD - NORDESTE:\n")
verificar_sihrd_disponibilidade('MA')  # Maranhão
verificar_sihrd_disponibilidade('PI')  # Piauí
verificar_sihrd_disponibilidade('CE')  # Ceará
verificar_sihrd_disponibilidade('RN')  # Rio Grande do Norte
verificar_sihrd_disponibilidade('PB')  # Paraíba
verificar_sihrd_disponibilidade('PE')  # Pernambuco
verificar_sihrd_disponibilidade('AL')  # Alagoas
verificar_sihrd_disponibilidade('SE')  # Sergipe
verificar_sihrd_disponibilidade('BA')  # Bahia

cat("\n🚀 Iniciando downloads SIH-RD - REGIÃO NORDESTE:\n")
# Download histórico completo - NORDESTE
baixar_sihrd_estado('MA')  # Maranhão - Histórico completo
baixar_sihrd_estado('PI')  # Piauí - Histórico completo
baixar_sihrd_estado('CE')  # Ceará - Histórico completo
baixar_sihrd_estado('RN')  # Rio Grande do Norte - Histórico completo
baixar_sihrd_estado('PB')  # Paraíba - Histórico completo
baixar_sihrd_estado('PE')  # Pernambuco - Histórico completo
baixar_sihrd_estado('AL')  # Alagoas - Histórico completo
baixar_sihrd_estado('SE')  # Sergipe - Histórico completo
baixar_sihrd_estado('BA')  # Bahia - Histórico completo

# ==============================
# REGIÃO CENTRO-OESTE (4 ESTADOS)
# ==============================
cat("\n🌾 === REGIÃO CENTRO-OESTE ===\n")
cat("Estados: MT, MS, GO, DF\n\n")

# Verificar disponibilidade - CENTRO-OESTE
cat("🔍 Verificando disponibilidade SIH-RD - CENTRO-OESTE:\n")
verificar_sihrd_disponibilidade('MT')  # Mato Grosso
verificar_sihrd_disponibilidade('MS')  # Mato Grosso do Sul
verificar_sihrd_disponibilidade('GO')  # Goiás
verificar_sihrd_disponibilidade('DF')  # Distrito Federal

cat("\n🚀 Iniciando downloads SIH-RD - REGIÃO CENTRO-OESTE:\n")
# Download histórico completo - CENTRO-OESTE
baixar_sihrd_estado('MT')  # Mato Grosso - Histórico completo
baixar_sihrd_estado('MS')  # Mato Grosso do Sul - Histórico completo
baixar_sihrd_estado('GO')  # Goiás - Histórico completo
baixar_sihrd_estado('DF')  # Distrito Federal - Histórico completo

# ==============================
# REGIÃO SUDESTE (4 ESTADOS)
# ==============================
cat("\n🏙️ === REGIÃO SUDESTE ===\n")
cat("Estados: ES, MG, RJ, SP\n\n")

# Verificar disponibilidade - SUDESTE
cat("🔍 Verificando disponibilidade SIH-RD - SUDESTE:\n")
verificar_sihrd_disponibilidade('ES')  # Espírito Santo
verificar_sihrd_disponibilidade('MG')  # Minas Gerais
verificar_sihrd_disponibilidade('RJ')  # Rio de Janeiro
verificar_sihrd_disponibilidade('SP')  # São Paulo

cat("\n🚀 Iniciando downloads SIH-RD - REGIÃO SUDESTE:\n")
# Download histórico completo - SUDESTE
baixar_sihrd_estado('ES')  # Espírito Santo - Histórico completo
baixar_sihrd_estado('MG')  # Minas Gerais - Histórico completo
baixar_sihrd_estado('RJ')  # Rio de Janeiro - Histórico completo
baixar_sihrd_estado('SP')  # São Paulo - Histórico completo

# ==============================
# REGIÃO SUL (3 ESTADOS)
# ==============================
cat("\n❄️ === REGIÃO SUL ===\n")
cat("Estados: PR, RS, SC\n\n")

# Verificar disponibilidade - SUL
cat("🔍 Verificando disponibilidade SIH-RD - SUL:\n")
verificar_sihrd_disponibilidade('PR')  # Paraná
verificar_sihrd_disponibilidade('RS')  # Rio Grande do Sul
verificar_sihrd_disponibilidade('SC')  # Santa Catarina

cat("\n🚀 Iniciando downloads SIH-RD - REGIÃO SUL:\n")
# Download histórico completo - SUL
baixar_sihrd_estado('PR')  # Paraná - Histórico completo
baixar_sihrd_estado('RS')  # Rio Grande do Sul - Histórico completo
baixar_sihrd_estado('SC')  # Santa Catarina - Histórico completo