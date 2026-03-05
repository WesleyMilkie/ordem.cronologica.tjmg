"""
Worker individual que processa uma entidade por vez.
Cada worker reclama a próxima entidade pendente do banco e processa ela.
Segue o mesmo fluxo de tjmg.py mas para uma entidade por vez.
"""

import sys
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime
import time

# Importar funções do tjmg.py
sys.path.insert(0, str(Path(__file__).parent))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# Importar funções do tjmg.py
from tjmg import (
    extrair_informacoes_modal,
    extrair_tabela_resultado,
    exibir_informacoes_tabular,
    extrair_total_precatorios,
    salvar_validacao,
    executar_auditoria_extracao,
    imprimir_resumo_auditoria,
)


def encontrar_primeiro_elemento(driver, seletores, timeout=10):
    for by, valor in seletores:
        try:
            return WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, valor))
            )
        except Exception:
            continue
    return None


def obter_conexao_local():
    base_dir = Path(__file__).resolve().parent
    env_path = base_dir / ".env"
    load_dotenv(env_path)

    user = os.getenv("TJPB_DB_USER")
    password = os.getenv("TJPB_DB_PASSWORD")
    dbname = os.getenv("TJPB_DB_NAME")
    host = os.getenv("TJPB_DB_HOST")
    port = os.getenv("TJPB_DB_PORT", "5432")

    if not all([user, password, dbname, host, port]):
        raise ValueError("Credenciais incompletas no .env")

    return psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )


def reivindicar_entidade(worker_id, worker_uuid):
    """Tenta reivindicar a próxima entidade não processada."""
    conn = obter_conexao_local()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Usar FOR UPDATE para pegar e bloquear atomicamente
            cur.execute(
                sql.SQL(
                    """
                    UPDATE ordens_cronologicas.entidades_controle
                    SET status = 'processando', worker_id = %s, data_inicio_processamento = %s
                    WHERE id = (
                        SELECT id FROM ordens_cronologicas.entidades_controle
                        WHERE status = 'pendente'
                        ORDER BY id ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, entidade_num, ente_devedor_principal, ultima_pagina_extraida
                    """
                ),
                (worker_uuid, datetime.now()),
            )
            result = cur.fetchone()
            conn.commit()

        if result:
            entidade_id, entidade_num, ente_devedor, ultima_pagina = result
            print(
                f"\n✅ Worker {worker_id} ({worker_uuid}) reivindicou: Entidade #{entidade_num} - {ente_devedor}"
            )
            pagina_inicio = ultima_pagina + 1 if ultima_pagina else 1
            return entidade_num, ente_devedor, pagina_inicio
        else:
            print(f"⏸️  Worker {worker_id} ({worker_uuid}) - Nenhuma entidade pendente")
            return None, None, 1

    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao reivindicar entidade (Worker {worker_id}): {e}")
        return None, None, 1
    finally:
        conn.close()


def marcar_entidade_como_concluida(entidade_num, total_registros, worker_uuid):
    """Marca a entidade como processada."""
    conn = obter_conexao_local()
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                UPDATE ordens_cronologicas.entidades_controle
                SET status = 'processada', data_conclusao = %s, total_registros_extraidos = %s
                WHERE entidade_num = %s
                """
            ),
            (datetime.now(), total_registros, entidade_num),
        )

    conn.close()
    print(f"✅ Entidade #{entidade_num} marcada como PROCESSADA")


def atualizar_checkpoint(entidade_num, pagina):
    """Atualiza o checkpoint de página para a entidade."""
    conn = obter_conexao_local()
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                UPDATE ordens_cronologicas.entidades_controle
                SET ultima_pagina_extraida = %s
                WHERE entidade_num = %s
                """
            ),
            (pagina, entidade_num),
        )

    conn.close()


def processar_entidade_completa(
    worker_id, worker_uuid, entidade_num, ente_devedor, pagina_inicio
):
    """
    Processa uma entidade completa seguindo o mesmo fluxo de tjmg.py.
    """
    url = "https://www8.tjmg.jus.br/juridico/pe/listaCronologia.jsf"

    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--headless")  # Descomente para modo headless

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Erro ao inicializar com gerenciamento automático: {e}")
        print("Tentando com webdriver-manager...")

        driver_path = ChromeDriverManager(cache_valid_range=0).install()
        print(f"Driver baixado em: {driver_path}")

        driver = webdriver.Chrome(service=Service(driver_path), options=options)

    wait = WebDriverWait(driver, 30)

    print(f"\n{'='*70}")
    print(f"🔍 Worker {worker_id} - PROCESSANDO ENTIDADE #{entidade_num}")
    print(f"{'='*70}")

    total_registros = 0
    sucesso = False

    try:
        driver.maximize_window()
        print(f"Acessando: {url}")
        driver.get(url)
        time.sleep(2)

        # ✅ Passo 1: Clicar no dropdown de entidades
        print("\n[PASSO 1] Abrindo dropdown de entidades...")
        dropdown_button = wait.until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "span.ui-button-icon-primary.ui-icon.ui-icon-triangle-1-s",
                )
            )
        )
        dropdown_button.click()
        time.sleep(1)

        # ✅ Passo 2: Aguardar a lista de entidades carregar
        print("[PASSO 2] Aguardando lista de entidades...")
        lista_entidades = encontrar_primeiro_elemento(
            driver,
            [
                (By.XPATH, '//*[@id="entidade_devedora_panel"]/ul'),
                (By.CSS_SELECTOR, "#entidade_devedora_panel ul"),
            ],
            timeout=30,
        )
        if not lista_entidades:
            raise Exception("lista de entidades não encontrada")

        # ✅ Passo 3: Encontrar todas as entidades
        print("[PASSO 3] Localizando entidades...")
        entidades = driver.find_elements(
            By.XPATH, '//*[@id="entidade_devedora_panel"]/ul/li'
        )
        if not entidades:
            entidades = driver.find_elements(
                By.CSS_SELECTOR, "#entidade_devedora_panel ul li"
            )
        print(f"📋 Encontradas {len(entidades)} entidades no dropdown")

        if entidade_num > len(entidades):
            raise ValueError(
                f"Entidade #{entidade_num} não existe (índice fora do intervalo: máx {len(entidades)})"
            )

        # ✅ Passo 4: Clicar na entidade específica
        print(f"[PASSO 4] Selecionando entidade #{entidade_num}...")
        entidade_element = entidades[entidade_num - 1]  # 1-indexed para 0-indexed
        driver.execute_script("arguments[0].scrollIntoView(true);", entidade_element)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", entidade_element)
        time.sleep(1.5)

        # ✅ Passo 5: Confirmar com Enter/Consultar
        print("[PASSO 5] Confirmando com Enter/Consultar...")
        try:
            ActionChains(driver).send_keys(Keys.ENTER).perform()
            print("✓ Enter pressionado")
        except:
            try:
                botao_consulta = WebDriverWait(driver, 10).until(
                    lambda d: encontrar_primeiro_elemento(
                        d,
                        [
                            (By.XPATH, '//*[@id="consulta2"]'),
                            (By.CSS_SELECTOR, "#consulta2"),
                            (
                                By.XPATH,
                                '//button[contains(normalize-space(.),"Consultar")] | //a[contains(normalize-space(.),"Consultar")]',
                            ),
                        ],
                        timeout=2,
                    )
                )
                botao_consulta.click()
                print("✓ Botão Consultar clicado")
            except:
                botao_consulta = encontrar_primeiro_elemento(
                    driver,
                    [
                        (By.XPATH, '//*[@id="consulta2"]'),
                        (By.CSS_SELECTOR, "#consulta2"),
                        (
                            By.XPATH,
                            '//button[contains(normalize-space(.),"Consultar")] | //a[contains(normalize-space(.),"Consultar")]',
                        ),
                    ],
                    timeout=3,
                )
                if not botao_consulta:
                    raise Exception("botão consultar não encontrado")
                driver.execute_script("arguments[0].click();", botao_consulta)
                print("✓ Botão Consultar clicado via JavaScript")

        # ✅ Passo 6: Aguardar processamento
        print("[PASSO 6] Aguardando processamento da consulta...")
        time.sleep(4)

        # ✅ Passo 7: Extrair informações do modal
        print("[PASSO 7] Extraindo informações da entidade...")
        informacoes = extrair_informacoes_modal(driver)
        exibir_informacoes_tabular(informacoes, entidade_num)

        # ✅ Passo 7.5: Extrair total de precatórios para validação
        print(f"\n[PASSO 7.5] Extraindo total de precatórios para validação...")
        total_precatorios = extrair_total_precatorios(driver)
        if total_precatorios is not None:
            salvar_validacao(
                entidade_num=entidade_num,
                ente_devedor_principal=informacoes.get("Ente Devedor Principal", ""),
                ente_devedor=informacoes.get("Ente Devedor", ""),
                total_precatorios=total_precatorios,
                worker_id=worker_uuid,
            )

        # ✅ Passo 8: Extrair dados da tabela de resultado
        dados_tabela_resultado = {}
        linha_inicio = 0

        if not informacoes.get("ordem_cronologica_nao_disponivel"):
            print(f"\n[PASSO 8] Extraindo tabela de resultados...")
            if pagina_inicio > 1:
                print(f"  ⏩ Retomando a partir da página {pagina_inicio}")
                linha_inicio = 0

            # Callback para salvar checkpoint após cada página
            def callback_checkpoint(pagina):
                atualizar_checkpoint(entidade_num, pagina)

            dados_tabela_resultado = extrair_tabela_resultado(
                driver,
                wait,
                informacoes,
                iniciar_de_pagina=pagina_inicio,
                iniciar_de_linha=linha_inicio,
                callback_checkpoint=callback_checkpoint,
            )
        else:
            print(f"\n[PASSO 8] ⏩ Pulando extração (ordem cronológica não disponível)")
            dados_tabela_resultado = {"headers": [], "registros": []}

        # ✅ Passo 9: Contar registros extraídos
        if dados_tabela_resultado and dados_tabela_resultado.get("registros"):
            total_registros = len(dados_tabela_resultado["registros"])
            print(f"\n📈 Total de registros extraídos: {total_registros}")
            # NÃO SALVAR NOVAMENTE - extrair_tabela_resultado já salva incrementalmente!
        else:
            total_registros = 0
            print(f"\n⚠️ Nenhum registro encontrado na tabela resultado")

        # ✅ Passo 10: Auditoria SQL da extração da entidade atual
        try:
            rows_auditoria = executar_auditoria_extracao(
                limite=400, entidade_num=entidade_num
            )
            imprimir_resumo_auditoria(
                rows_auditoria,
                entidade_num=entidade_num,
                ente_devedor=informacoes.get("Ente Devedor", ente_devedor),
            )
        except Exception as auditoria_error:
            print(f"⚠️ Falha ao executar auditoria de extração: {auditoria_error}")

        sucesso = True

    except Exception as e:
        print(f"\n❌ Erro ao processar entidade: {e}")
        import traceback

        traceback.print_exc()
        raise

    finally:
        driver.quit()

    return sucesso, total_registros


def main():
    if len(sys.argv) < 3:
        print("Uso: python tjmg_worker.py <worker_id> <worker_uuid>")
        sys.exit(1)

    worker_id = int(sys.argv[1])
    worker_uuid = sys.argv[2]

    print(f"\n🚀 Worker {worker_id} iniciado (UUID: {worker_uuid})")

    # Loop: reivindicar e processar entidades até não haver mais
    while True:
        entidade_num, ente_devedor, pagina_inicio = reivindicar_entidade(
            worker_id, worker_uuid
        )

        if entidade_num is None:
            print(f"\n🛑 Worker {worker_id} - Finalizando (sem mais entidades)")
            break

        try:
            sucesso, total_registros = processar_entidade_completa(
                worker_id, worker_uuid, entidade_num, ente_devedor, pagina_inicio
            )
            if sucesso:
                marcar_entidade_como_concluida(
                    entidade_num, total_registros, worker_uuid
                )
        except Exception as e:
            print(f"\n❌ Erro fatal ao processar entidade #{entidade_num}: {e}")
            # Marcar como erro e continuar com próxima
            try:
                conn = obter_conexao_local()
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "UPDATE ordens_cronologicas.entidades_controle SET status = 'erro' WHERE entidade_num = %s"
                        ),
                        (entidade_num,),
                    )
                conn.close()
            except Exception as db_error:
                print(f"⚠️ Erro ao marcar como erro: {db_error}")

    print(f"\n✅ Worker {worker_id} finalizado\n")


if __name__ == "__main__":
    main()
