# Registro da publicação

    - Fonte: `nordeste-health-lakehouse (1)(1).dbc`.
    - SHA-256 da fonte: `f0720fed794fb2400991fb762f19c616176073fe0d72dd70eaf3e33f777b2084`.
    - Data: 30/09/2026.
    - Destino: `RowerBomfim/bootcamp-databricks-capgemini`, pasta
      `projetos/nordeste-health-lakehouse`.
    - Formato: notebooks Databricks SOURCE Python com células SQL e Markdown.

    ## Ajustes aplicados

    Os números a seguir correspondem às células do DBC enviado, incluindo Markdown.

    | Notebook | Ajuste | Motivo |
    | --- | --- | --- |
    | Tratamento_Silver | Mover a célula 8 de evidência temporal para depois da configuração da célula 3 | Avaliar a evidência antes da validação e dos filtros |
    | Modelo_Gold | Mover a célula SQL 13 para depois da reconciliação da célula 17 | Comentar e consultar fatos após sua criação e conferência |
    | Analytics_Validação | Remover a célula 21, igual à célula 20 | Executar uma única vez a validação e a recriação das views |

    As células únicas foram preservadas integralmente. Não foram preenchidas
    metas, grupos, caminhos de Job ou referências adicionais. As fórmulas dos
    indicadores e o teste sintético de consolidação da Gold são os da fonte.

    ## Verificações locais

    | Arquivo | Células publicadas | Células Python |
    | --- | ---: | ---: |
    | `01_Ingestao_Bronze.py` | 22 | 7 |
| `02_Tratamento_Silver.py` | 21 | 12 |
| `03_Modelo_Gold.py` | 20 | 12 |
| `04_Analytics_Validacao.py` | 31 | 12 |

    - Integridade ZIP do DBC conferida.
    - Sintaxe das 43 células Python conferida.
    - Sintaxe dos quatro arquivos SOURCE conferida.
    - Conversão SOURCE reconstruída para conferir conteúdo e limites das células.
    - Posições da evidência temporal e da view Gold conferidas.
    - Remoção da célula duplicada conferida por comparação de conteúdo.
    - Links relativos do README conferidos contra os arquivos publicados.
    - Código convertido verificado sem e-mails, tokens ou chaves privadas aparentes.

    O DBC inclui saídas de execuções anteriores. Os totais de auditoria citados no
    README foram extraídos dessas saídas. A execução integral de Spark, Delta,
    permissões e Job não foi realizada nesta preparação para o GitHub.
