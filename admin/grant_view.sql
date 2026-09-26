-- Execute manualmente no Databricks somente após validar o grupo autorizado.
-- Substitua grupo_consumidores_autorizados pelo nome real do grupo no Unity Catalog.
-- A view deve ter sido criada pelo notebook principal antes destes comandos.
-- Não publique o nome real do grupo nem o resultado de SHOW GRANTS no GitHub.

GRANT USE CATALOG ON CATALOG rower_bootcamp TO `grupo_consumidores_autorizados`;
GRANT USE SCHEMA ON SCHEMA rower_bootcamp.gold TO `grupo_consumidores_autorizados`;
GRANT SELECT ON VIEW rower_bootcamp.gold.vw_top_10_brands TO `grupo_consumidores_autorizados`;
