import argparse
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


URL_TJMG = "https://www8.tjmg.jus.br/juridico/pe/listaCronologia.jsf"
MSG_INDISPONIVEL = "Ordem Cronológica de Pagamento não disponível nesta consulta!"


def encontrar_primeiro(driver, seletores, timeout=8):
    for by, value in seletores:
        try:
            el = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return el, by, value
        except Exception:
            continue
    return None, None, None


def texto_elemento(el):
    if not el:
        return ""
    txt = (el.get_attribute("value") or "").strip()
    if txt:
        return txt
    return (el.text or "").strip()


def print_resultado(titulo, by, value, valor, ok):
    print("\n" + "=" * 90)
    print(f"CAMPO: {titulo}")
    print(f"SELETOR: {by} -> {value}")
    print(f"STATUS: {'OK' if ok else 'FALHOU'}")
    print(f"VALOR: {valor if valor else '<vazio>'}")
    if valor:
        print(f"TAMANHO_TEXTO: {len(valor)}")
        print(f"RAW_REPR: {repr(valor)}")
    print("=" * 90)


def abrir_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    try:
        return webdriver.Chrome(options=options)
    except Exception:
        path = ChromeDriverManager(cache_valid_range=0).install()
        return webdriver.Chrome(service=Service(path), options=options)


def selecionar_entidade(driver, wait, entidade_num):
    print("\n[1] Abrindo página do TJMG...")
    driver.get(URL_TJMG)
    time.sleep(2)

    print("[2] Abrindo dropdown de entidades...")
    dropdown = wait.until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                "span.ui-button-icon-primary.ui-icon.ui-icon-triangle-1-s",
            )
        )
    )
    dropdown.click()
    time.sleep(1)

    print("[3] Carregando lista de entidades...")
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#entidade_devedora_panel ul"))
    )
    entidades = driver.find_elements(By.CSS_SELECTOR, "#entidade_devedora_panel ul li")

    if entidade_num < 1 or entidade_num > len(entidades):
        raise ValueError(
            f"Entidade inválida: {entidade_num}. Intervalo disponível: 1..{len(entidades)}"
        )

    entidade = entidades[entidade_num - 1]
    print(f"[4] Entidade selecionada: #{entidade_num} -> {entidade.text}")
    driver.execute_script("arguments[0].scrollIntoView(true);", entidade)
    time.sleep(0.4)
    driver.execute_script("arguments[0].click();", entidade)
    time.sleep(1.0)

    print("[5] Confirmando consulta...")
    try:
        ActionChains(driver).send_keys(Keys.ENTER).perform()
    except Exception:
        botao_consulta = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#consulta2"))
        )
        botao_consulta.click()

    time.sleep(4)


def validar_campos(driver):
    testes = [
        {
            "titulo": "Ente Devedor Principal",
            "seletores": [
                (By.XPATH, '//*[@id="entePrincipal"]'),
                (By.CSS_SELECTOR, "#entePrincipal"),
            ],
        },
        {
            "titulo": "Ente Devedor",
            "seletores": [
                (By.XPATH, '//*[@id="enteDevedor"]'),
                (By.CSS_SELECTOR, "#enteDevedor"),
            ],
        },
        {
            "titulo": "Regime de Pagamento",
            "seletores": [
                (By.XPATH, '//*[@id="regimePagamento"]'),
                (By.CSS_SELECTOR, "#regimePagamento"),
            ],
        },
        {
            "titulo": "Lei Pequeno Valor",
            "seletores": [
                (By.XPATH, '//*[@id="leiPequenoValor"]'),
                (By.CSS_SELECTOR, "#leiPequenoValor"),
            ],
        },
        {
            "titulo": "Mensagem de indisponibilidade",
            "seletores": [
                (By.XPATH, '//*[@id="j_idt23"]/div[1]/ul/li/span'),
                (
                    By.XPATH,
                    '//*[contains(normalize-space(.),"Ordem Cronológica de Pagamento não disponível nesta consulta!")]',
                ),
            ],
        },
        {
            "titulo": "Footer total de precatórios",
            "seletores": [
                (
                    By.CSS_SELECTOR,
                    "#resultado > div.ui-datatable-footer.ui-widget-header.ui-corner-bottom",
                ),
                (
                    By.XPATH,
                    '//*[@id="resultado"]//div[contains(@class,"ui-datatable-footer")]',
                ),
                (By.XPATH, '//*[@id="resultado"]/div[5]'),
                (By.XPATH, '//*[@id="resultado"]/div[3]'),
            ],
        },
        {
            "titulo": "Cabeçalho da tabela",
            "seletores": [
                (By.XPATH, '//*[@id="resultado_head"]'),
                (By.CSS_SELECTOR, "#resultado_head"),
            ],
        },
        {
            "titulo": "Linhas da tabela",
            "seletores": [
                (By.XPATH, '//*[@id="resultado_data"]/tr'),
                (By.CSS_SELECTOR, "#resultado_data > tr"),
            ],
        },
    ]

    print("\n[6] Iniciando validação incremental de XPaths/seletores...")
    for item in testes:
        el, by, value = encontrar_primeiro(driver, item["seletores"], timeout=5)
        valor = texto_elemento(el)
        ok = el is not None

        if item["titulo"] == "Linhas da tabela":
            linhas = driver.find_elements(By.CSS_SELECTOR, "#resultado_data > tr")
            primeira_linha = ""
            segunda_linha = ""
            if linhas:
                colunas = linhas[0].find_elements(By.TAG_NAME, "td")
                valores = [c.text.strip() for c in colunas[1:] if c.text.strip()]
                primeira_linha = " | ".join(valores)
            if len(linhas) > 1:
                colunas = linhas[1].find_elements(By.TAG_NAME, "td")
                valores = [c.text.strip() for c in colunas[1:] if c.text.strip()]
                segunda_linha = " | ".join(valores)

            valor = f"{len(linhas)} linha(s)"
            if primeira_linha:
                valor += f" | primeira_linha: {primeira_linha}"
            if segunda_linha:
                valor += f" | segunda_linha: {segunda_linha}"

        if item["titulo"] == "Mensagem de indisponibilidade":
            if MSG_INDISPONIVEL in valor:
                ok = True
            else:
                ok = False
                valor = (
                    f"{valor} | esperado conter: '{MSG_INDISPONIVEL}'"
                    if valor
                    else f"<não encontrado> | esperado: '{MSG_INDISPONIVEL}'"
                )

        print_resultado(item["titulo"], by, value, valor, ok)


def validar_linha_detalhes(driver, linha_num):
    print(f"\n[7] Validando próximos XPaths (interações da {linha_num}ª linha)...")

    linhas = driver.find_elements(By.CSS_SELECTOR, "#resultado_data > tr")
    if len(linhas) < linha_num:
        print_resultado(
            f"Pré-condição: {linha_num}ª linha da tabela",
            "css selector",
            "#resultado_data > tr",
            f"Linha {linha_num} não disponível para validar detalhes",
            False,
        )
        return

    row_zero = linha_num - 1

    # 1) Botão de expansão da primeira linha
    seletores_expandir = [
        (
            By.XPATH,
            f'//*[@id="resultado_data"]/tr[{linha_num}]/td[1]//*[self::div or self::a or self::span]',
        ),
        (
            By.CSS_SELECTOR,
            f"#resultado_data > tr:nth-child({linha_num}) > td:first-child div",
        ),
    ]
    btn_expandir, by, value = encontrar_primeiro(driver, seletores_expandir, timeout=3)
    ok_expandir = btn_expandir is not None
    valor_expandir = "botão de expandir encontrado"

    if ok_expandir:
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", btn_expandir)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", btn_expandir)
            valor_expandir = "botão clicado com sucesso"
            time.sleep(1.2)
        except Exception as e:
            ok_expandir = False
            valor_expandir = f"erro no clique: {e}"

    print_resultado(
        f"Expandir {linha_num}ª linha", by, value, valor_expandir, ok_expandir
    )

    # 2) Subtabela da primeira linha
    seletores_subtabela = [
        (By.XPATH, f'//*[@id="resultado:{row_zero}:j_idt45_content"]'),
        (By.XPATH, f'//*[@id="resultado:{row_zero}:j_idt46:display"]'),
        (
            By.XPATH,
            f'//*[@id="resultado"]//*[contains(@id,":{row_zero}:") and (contains(@id,"content") or contains(@id,"display"))]',
        ),
    ]
    el_sub, by_sub, value_sub = encontrar_primeiro(
        driver, seletores_subtabela, timeout=3
    )
    print_resultado(
        f"Subtabela {linha_num}ª linha",
        by_sub,
        value_sub,
        texto_elemento(el_sub),
        el_sub is not None,
    )

    # 3) Botão idAndamento da primeira linha
    seletores_andamento = [
        (By.XPATH, f'//*[@id="resultado:{row_zero}:idAndamento"]'),
        (
            By.XPATH,
            f'//*[@id="resultado_data"]/tr[{linha_num}]//*[contains(@id,":idAndamento")]',
        ),
        (
            By.XPATH,
            f'//*[@id="resultado_data"]/tr[{linha_num}]//a[contains(translate(normalize-space(.), "ANDAMENTO", "andamento"), "andamento")]',
        ),
    ]
    btn_and, by_and, value_and = encontrar_primeiro(
        driver, seletores_andamento, timeout=3
    )
    ok_and = btn_and is not None
    valor_and = "botão de andamento encontrado"

    if ok_and:
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", btn_and)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", btn_and)
            valor_and = "botão clicado com sucesso"
            time.sleep(2)
        except Exception as e:
            ok_and = False
            valor_and = f"erro no clique: {e}"

    print_resultado(
        f"Botão Andamento {linha_num}ª linha", by_and, value_and, valor_and, ok_and
    )

    # 4) Painel de detalhes após clicar andamento
    seletores_painel = [
        (By.XPATH, '//*[@id="frm_detalhe:j_idt91:j_idt104"]'),
        (By.XPATH, '//*[@id="frm_detalhe"]//*[contains(@id,":j_idt104")]'),
        (
            By.XPATH,
            '//*[@id="frm_detalhe"]//*[contains(normalize-space(.),"Credor Principal:")]',
        ),
    ]
    painel, by_p, value_p = encontrar_primeiro(driver, seletores_painel, timeout=5)
    print_resultado(
        f"Painel de detalhes (linha {linha_num})",
        by_p,
        value_p,
        texto_elemento(painel),
        painel is not None,
    )

    # 5) Bloco de beneficiários
    seletores_benef = [
        (By.XPATH, '//*[@id="frm_detalhe:j_idt91:crontrol_lblBeneficiario"]'),
        (By.XPATH, '//*[@id="frm_detalhe"]//*[contains(@id,"Beneficiario")]'),
        (
            By.XPATH,
            '//*[@id="frm_detalhe"]//*[contains(normalize-space(.),"Beneficiários")]',
        ),
    ]
    ben, by_b, value_b = encontrar_primeiro(driver, seletores_benef, timeout=3)
    print_resultado(
        f"Beneficiários (modal, linha {linha_num})",
        by_b,
        value_b,
        texto_elemento(ben),
        ben is not None,
    )

    # 6) Fechar modal de detalhes
    seletores_fechar = [
        (By.XPATH, '//*[@id="frm_detalhe:j_idt91:j_idt100"]/span'),
        (
            By.CSS_SELECTOR,
            "#frm_detalhe a.ui-dialog-titlebar-close, #frm_detalhe span.ui-icon-closethick",
        ),
        (
            By.XPATH,
            '//*[@id="frm_detalhe"]//*[contains(@class,"ui-dialog-titlebar-close") or contains(@class,"ui-icon-closethick")]',
        ),
    ]
    btn_close, by_c, value_c = encontrar_primeiro(driver, seletores_fechar, timeout=3)
    ok_close = btn_close is not None
    valor_close = "botão de fechar encontrado"

    if ok_close:
        try:
            driver.execute_script("arguments[0].click();", btn_close)
            valor_close = "modal fechado com sucesso"
            time.sleep(1)
        except Exception as e:
            ok_close = False
            valor_close = f"erro ao fechar modal: {e}"

    print_resultado(
        f"Fechar modal de detalhes (linha {linha_num})",
        by_c,
        value_c,
        valor_close,
        ok_close,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Valida XPaths/seletores do TJMG por etapas, com log detalhado."
    )
    parser.add_argument(
        "--entidade",
        type=int,
        default=1,
        help="Número da entidade para teste (1-based). Ex.: --entidade 47",
    )
    args = parser.parse_args()

    driver = abrir_driver()
    wait = WebDriverWait(driver, 20)

    try:
        driver.maximize_window()
        selecionar_entidade(driver, wait, args.entidade)
        validar_campos(driver)
        validar_linha_detalhes(driver, 1)
        validar_linha_detalhes(driver, 2)

        print(
            "\n✅ Validação finalizada. Próximo passo: ajustar os seletores que falharam."
        )
    except Exception as e:
        print(f"\n❌ Erro durante validação: {e}")
    finally:
        print("\nFechando navegador...")
        driver.quit()


if __name__ == "__main__":
    main()
