# Mini Lakehouse: análise de vendas de e-commerce

Projeto desenvolvido durante o bootcamp de Databricks da Capgemini. O notebook implementa ingestão incremental, tratamento de dados e indicadores de vendas em um Lakehouse com Unity Catalog, Delta Lake e arquitetura Medalhão.

## Arquitetura

| Camada | Objetivo | Principais objetos |
| --- | --- | --- |
| Landing | Receber arquivos `sales*.csv` e `products*.csv` | Volume `rower_bootcamp.landing.files` |
| Bronze | Ingerir arquivos com Auto Loader e checkpoints persistentes | `bronze.orders_raw`, `bronze.products_raw` |
| Silver | Tipar, deduplicar, validar e separar registros inválidos | `silver.sales`, `silver.sales_quarantine` |
| Gold | Consolidar indicadores para análise | `gold.monthly_sales_indicators`, `gold.brand_sales_ranking`, `gold.customer_segment_performance` |
| Consumo | Expor ranking por uma view | `gold.vw_top_10_brands` |

O notebook também verifica idempotência, ingestão de novos lotes, qualidade e reconciliação entre Silver e Gold.

## Como executar no Databricks

1. Use uma workspace com Unity Catalog e computação compatível com Auto Loader. Confirme permissão para criar o catálogo `rower_bootcamp`, os schemas e o volume; se o catálogo já existir, confirme acesso a ele.
2. Importe `notebooks/Indian_Ecommerce_Lakehouse.py` como notebook Databricks. O arquivo usa a sintaxe de exportação Databricks (`# COMMAND ----------` e `# MAGIC`).
3. Envie os CSVs autorizados para `/Volumes/rower_bootcamp/landing/files/desafio/`. O notebook espera arquivos com prefixos `sales` e `products` e os cabeçalhos definidos no código.
4. Execute as células na ordem. As células de teste incremental criam e reinicializam objetos de teste em `_tests/incremental_orders` e `bronze.orders_raw_incremental_test`; confira esses caminhos antes de executá-las em outro ambiente.
5. Confira as contagens, os testes de qualidade e a reconciliação ao final. O código-fonte pode ser inspecionado aqui; **não há confirmação de execução deste commit em uma workspace Databricks**.

O nome do catálogo e os caminhos de volume estão fixados no notebook para reproduzir o ambiente do projeto. Para usar outro catálogo, atualize as referências antes da primeira execução.

## Governança e publicação segura

- O notebook não concede permissões durante `Run All`. Para liberar a view a um grupo autorizado, um administrador deve revisar e executar manualmente `admin/grant_view.sql` após substituir o nome ilustrativo.
- Este repositório contém apenas código e documentação. Não envie CSVs de origem, dados de clientes, checkpoints, segredos, credenciais, saídas de células ou arquivos `.dbc` exportados com resultados.
- Mantenha auditorias de `SHOW GRANTS`, metadados da workspace e evidências com e-mails em ambiente privado. Se precisar de capturas públicas, oculte nomes de usuários e detalhes dos caminhos.
- A Bronze e a Silver armazenam campos como `Customer_ID`, idade e texto de avaliações. Controle o acesso aos dados no Unity Catalog e exponha somente as colunas necessárias ao público consumidor.

## Estrutura

```text
notebooks/Indian_Ecommerce_Lakehouse.py  Notebook principal
admin/grant_view.sql                   Exemplo de concessão manual
.gitignore                             Bloqueio de dados e exportações locais
```

**Autor:** Rower Bomfim. Projeto de estudo; a referência ao bootcamp identifica o contexto de aprendizagem.
