# ordem.cronologica.tjmg

Automação de extração e validação de dados de ordens cronológicas do TJMG, com suporte a execução paralela por workers e persistência em PostgreSQL.

## Visão geral

Este repositório contém scripts para:
- extrair dados por ente devedor;
- salvar resultados na tabela de extração;
- rodar validações e auditorias de qualidade dos dados;
- acompanhar status de filas e divergências.

## Ponto de entrada das docs

Documentação organizada no padrão Diátaxis:

- Tutoriais: [docs/tutorials](docs/tutorials/README.md)
- How-to: [docs/how-to](docs/how-to/README.md)
- Referência: [docs/reference](docs/reference/README.md)
- Explicações: [docs/explanations](docs/explanations/README.md)
- ADRs (decisões): [docs/adr](docs/adr/README.md)

## Estrutura do projeto (resumo)

- [tjmg.py](tjmg.py): fluxo principal de extração e auditoria.
- [tjmg_worker.py](tjmg_worker.py): worker para processar entidades em paralelo.
- [tjmg_paralelo.py](tjmg_paralelo.py): orquestrador de múltiplos workers.
- [tjmg_validacao_worker.py](tjmg_validacao_worker.py): worker de validação.
- [tjmg_validacao_paralelo.py](tjmg_validacao_paralelo.py): orquestrador da validação.

## Regras de documentação

- Cada documento tem um único objetivo (não misturar tutorial/how-to/reference/explanation).
- Quando faltar detalhe confirmado, registrar como `TODO`.
- README da raiz funciona como mapa, não como manual completo.

## Caso de múltiplas RPAs/tribunais (padrão)

Quando houver subpastas por tribunal, manter:
- README raiz com índice geral;
- README em cada subpasta (`rpas/<tribunal>/README.md`) com visão rápida, execução e links para docs locais.
