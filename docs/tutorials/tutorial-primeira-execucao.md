# Tutorial: primeira execução local

Objetivo: executar a automação pela primeira vez em ambiente local.

## Pré-requisitos

1. Python 3.11+ instalado.
2. Acesso ao banco PostgreSQL.
3. Arquivo `.env` preenchido.

## Passo a passo

1. Criar/ativar ambiente virtual.
2. Instalar dependências usadas pelos scripts.
3. Ajustar variáveis de ambiente no `.env`.
4. Validar conexão com banco via script de status.
5. Rodar extração principal.

## Resultado esperado

- Worker(s) iniciam sem erro de credenciais.
- Registros começam a aparecer em `ordens_cronologicas.tjmg`.

## TODO

- Detalhar lista fechada de dependências em arquivo dedicado.
- Adicionar exemplo completo de `.env` sem segredos reais.
