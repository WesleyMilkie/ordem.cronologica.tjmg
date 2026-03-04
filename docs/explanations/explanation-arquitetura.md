# Explanation: arquitetura e trade-offs

## Contexto

A extração precisa lidar com volume alto, variabilidade de tela e necessidade de rastreabilidade no banco.

## Decisões principais

- Execução por workers para ganho de throughput.
- Controle de fila em banco (`entidades_controle`) para distribuir entidades.
- Salvamento incremental para reduzir perda em caso de falha.
- Auditoria SQL para detectar duplicação e XPath instável.

## Trade-offs

- Mais workers aumentam velocidade, mas também custo de observabilidade e ruído de log.
- XPath muito genérico aumenta risco de mistura de itens.
- Salvamento incremental mal delimitado pode induzir duplicação.

## TODO

- Formalizar política de retry por tipo de erro Selenium.
- Definir SLO de execução por lote de entidades.
