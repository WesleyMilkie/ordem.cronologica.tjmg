# Reference: configuração e contratos técnicos

## Variáveis de ambiente

- `TJPB_DB_USER`
- `TJPB_DB_PASSWORD`
- `TJPB_DB_NAME`
- `TJPB_DB_HOST`
- `TJPB_DB_PORT` (default `5432`)

## Tabelas principais

- `ordens_cronologicas.tjmg`
- `ordens_cronologicas.entidades_controle`
- `ordens_cronologicas.tjmg_validacao`

## Scripts principais

- `tjmg.py`: extração principal e auditoria.
- `tjmg_worker.py`: worker por entidade.
- `tjmg_paralelo.py`: orquestrador de workers.
- `tjmg_validacao_worker.py`: validação por entidade.
- `tjmg_validacao_paralelo.py`: orquestrador de validações.

## SQL de auditoria

A auditoria de extração está embutida no fluxo e retorna seções:
- `A_RESUMO`
- `B_TOP_ITENS_REPETIDOS`
- `C_TOP_REPETICAO_MESMO_ALVO`
- `D_XPATH_INSTAVEL`

## TODO

- Documentar esquema completo de colunas de `ordens_cronologicas.tjmg`.
- Documentar índices esperados em produção.
