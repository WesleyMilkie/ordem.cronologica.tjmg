"""
Worker específico para extrair apenas o número de precatórios (validação).
Processa todas as entidades, independente do status de extração detalhada.
"""

import sys
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime
import time

sys.path.insert(0, str(Path(__file__).parent))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from tjmg import (
    extrair_informacoes_modal,
    extrair_total_precatorios,
    salvar_validacao,
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


def reivindicar_entidade_validacao(worker_id, worker_uuid):
    """Reivindica próxima entidade que ainda não tem validação extraída."""
    conn = obter_conexao_local()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Marca atomicamente com o worker_id para evitar conflitos
            cur.execute(
                sql.SQL(
                    """
                    UPDATE ordens_cronologicas.entidades_controle
                    SET validacao_worker_id = %s
                    WHERE id = (
                        SELECT id FROM ordens_cronologicas.entidades_controle
                        WHERE (validacao_extraida = FALSE OR validacao_extraida IS NULL)
                        AND validacao_worker_id IS NULL
                        ORDER BY id ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, entidade_num, ente_devedor_principal
                    """
                ),
                (worker_uuid,),
            )
            result = cur.fetchone()
            conn.commit()

        if result:
            entidade_id, entidade_num, ente_devedor = result
            print(
                f"\n✅ Worker {worker_id} ({worker_uuid}) - Validação #{entidade_num} - {ente_devedor}"
            )
            return entidade_num, ente_devedor
        else:
            print(f"⏸️  Worker {worker_id} ({worker_uuid}) - Nenhuma validação pendente")
            return None, None

    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao reivindicar validação (Worker {worker_id}): {e}")
        return None, None
    finally:
        conn.close()


def marcar_validacao_extraida(entidade_num):
    """Marca a entidade como validação extraída após sucesso."""
    try:
        conn = obter_conexao_local()
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """UPDATE ordens_cronologicas.entidades_controle 
                    SET validacao_extraida = TRUE, validacao_worker_id = NULL 
                    WHERE entidade_num = %s"""
                ),
                (entidade_num,),
            )
        conn.close()
    except Exception as e:
        print(f"Erro ao marcar validação extraída: {e}")


def extrair_validacao_entidade(worker_id, worker_uuid, entidade_num, ente_devedor):
    """Extrai apenas o número de precatórios da entidade (validação rápida)."""
    url = "https://www8.tjmg.jus.br/juridico/pe/listaCronologia.jsf"

    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--headless")  # Desabilitado para visualização

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Erro ao inicializar Chrome: {e}")
        driver_path = ChromeDriverManager(cache_valid_range=0).install()
        driver = webdriver.Chrome(service=Service(driver_path), options=options)

    wait = WebDriverWait(driver, 30)

    print(f"\n{'='*70}")
    print(f"🔍 Worker {worker_id} - VALIDAÇÃO ENTIDADE #{entidade_num}")
    print(f"{'='*70}")

    sucesso = False

    try:
        driver.maximize_window()

        # Carregar página inicial
        driver.get(url)
        time.sleep(2)

        # Abrir dropdown
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

        # Aguardar lista
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

        # Localizar entidades
        entidades = driver.find_elements(
            By.XPATH, '//*[@id="entidade_devedora_panel"]/ul/li'
        )
        if not entidades:
            entidades = driver.find_elements(
                By.CSS_SELECTOR, "#entidade_devedora_panel ul li"
            )

        if entidade_num > len(entidades):
            raise ValueError(f"Entidade #{entidade_num} não existe")

        # Selecionar entidade
        entidade_element = entidades[entidade_num - 1]
        driver.execute_script("arguments[0].scrollIntoView(true);", entidade_element)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", entidade_element)
        time.sleep(1.5)

        # Confirmar
        try:
            ActionChains(driver).send_keys(Keys.ENTER).perform()
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

        # Aguardar carregamento completo dos resultados
        time.sleep(5)

        # Aguardar pela tabela de resultados estar visível
        try:
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#resultado > div.ui-datatable-footer")
                )
            )
        except:
            pass

        # Delay adicional para garantir que todos os dados foram atualizados
        time.sleep(3)

        # Extrair informações básicas
        informacoes = extrair_informacoes_modal(driver)

        # Extrair total de precatórios
        total_precatorios = extrair_total_precatorios(driver)

        # Sempre salvar a validação, mesmo que total seja None
        # (indica que não há ordem cronológica disponível)
        salvar_validacao(
            entidade_num=entidade_num,
            ente_devedor_principal=informacoes.get("Ente Devedor Principal", ""),
            ente_devedor=informacoes.get("Ente Devedor", ""),
            total_precatorios=total_precatorios if total_precatorios is not None else 0,
            worker_id=worker_uuid,
        )

        if total_precatorios is not None:
            print(f"✅ Validação salva: {total_precatorios} precatórios")
        else:
            print(
                f"✅ Validação salva: 0 precatórios (ordem cronológica não disponível)"
            )

        # Marcar como extraída SEMPRE após salvar
        marcar_validacao_extraida(entidade_num)
        sucesso = True

    except Exception as e:
        print(f"❌ Erro ao processar validação: {e}")
        import traceback

        traceback.print_exc()

        # Mesmo em caso de erro, limpar o worker_id para não travar
        try:
            marcar_validacao_extraida(entidade_num)
        except:
            pass

    finally:
        driver.quit()

    return sucesso


def main():
    if len(sys.argv) < 3:
        print("Uso: python tjmg_validacao_worker.py <worker_id> <worker_uuid>")
        sys.exit(1)

    worker_id = int(sys.argv[1])
    worker_uuid = sys.argv[2]

    print(f"\n🚀 Worker Validação {worker_id} iniciado (UUID: {worker_uuid})")

    # Loop: processar validações até não haver mais
    while True:
        entidade_num, ente_devedor = reivindicar_entidade_validacao(
            worker_id, worker_uuid
        )

        if entidade_num is None:
            print(f"\n🛑 Worker {worker_id} - Finalizando (sem mais validações)")
            break

        try:
            sucesso = extrair_validacao_entidade(
                worker_id, worker_uuid, entidade_num, ente_devedor
            )
            # Se falhou, limpa o worker_id para permitir nova tentativa
            if not sucesso:
                try:
                    conn = obter_conexao_local()
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE ordens_cronologicas.entidades_controle SET validacao_worker_id = NULL WHERE entidade_num = %s",
                            (entidade_num,),
                        )
                    conn.close()
                except:
                    pass
                print(
                    f"⚠️ Validação da entidade #{entidade_num} não foi salva, liberada para nova tentativa"
                )
        except Exception as e:
            print(f"\n❌ Erro fatal na validação #{entidade_num}: {e}")
            # Limpa o worker_id para permitir nova tentativa
            try:
                conn = obter_conexao_local()
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE ordens_cronologicas.entidades_controle SET validacao_worker_id = NULL WHERE entidade_num = %s",
                        (entidade_num,),
                    )
                conn.close()
            except:
                pass

    print(f"\n✅ Worker Validação {worker_id} finalizado\n")


if __name__ == "__main__":
    main()
