# How-to: rodar workers em paralelo

Objetivo: iniciar extração com múltiplos workers.

## Checklist

1. Garantir fila com `status = 'pendente'` em `entidades_controle`.
2. Confirmar tabela de destino (`ordens_cronologicas.tjmg`) pronta.
3. Executar:

```bash
python tjmg_paralelo.py 2
```

4. Acompanhar logs dos workers.
5. Conferir evolução no banco (status, total inserido, auditoria).

## Variações

- Subir 4 workers: `python tjmg_paralelo.py 4`
- Subir 6 workers: `python tjmg_paralelo.py 6`

## TODO

- Definir limite recomendado de workers por máquina.
