import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import hashlib
import io
from datetime import datetime
import re
from collections import defaultdict
import xml.dom.minidom as minidom
import os
import zipfile
import time

@st.cache_data
def parse_xte(file):
    file.seek(0)
    content = file.read().decode('iso-8859-1')
    tree = ET.ElementTree(ET.fromstring(content))
    root = tree.getroot()
    ns = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}
    all_data = []
    
    colunas_para_manter = [
        'Nome da Origem', 'tipoRegistro', 'versaoTISSPrestador', 'formaEnvio', 'CNES',
        'identificadorExecutante', 'codigoCNPJ_CPF', 'municipioExecutante', 'numeroCartaoNacionalSaude',
        'cpfBeneficiario', 'sexo', 'dataNascimento', 'municipioResidencia', 'numeroRegistroPlano',
        'tipoEventoAtencao', 'origemEventoAtencao', 'numeroGuia_prestador', 'numeroGuia_operadora',
        'identificacaoReembolso', 'formaRemuneracao', 'valorRemuneracao', 'dataAutorizacao',
        'dataRealizacao', 'dataProtocoloCobranca', 'dataPagamento', 'dataProcessamentoGuia',
        'tipoConsulta', 'indicacaoRecemNato', 'indicacaoAcidente', 'caraterAtendimento',
        'tipoAtendimento', 'regimeAtendimento', 'valorTotalInformado', 'valorProcessado',
        'valorTotalPagoProcedimentos', 'valorTotalDiarias', 'valorTotalTaxas', 'valorTotalMateriais',
        'valorTotalOPME', 'valorTotalMedicamentos', 'valorGlosaGuia', 'valorPagoGuia',
        'valorPagoFornecedores', 'valorTotalTabelaPropria', 'valorTotalCoParticipacao',
        'codigoTabela', 'grupoProcedimento', 'quantidadeInformada', 'codigoProcedimento',
        'valorInformado', 'valorPagoProc', 'quantidadePaga', 'valorPagoFornecedor',
        'valorCoParticipacao', 'unidadeMedida', 'numeroGuiaSPSADTPrincipal', 'tipoInternacao',
        'regimeInternacao', 'diagnosticoCID', 'tipoFaturamento', 'motivoSaida', 'cboExecutante',
        'dataFimPeriodo', 'declaracaoObito', 'declaracaoNascido', 'Idade_na_Realização',
        'registroANSOperadoraIntermediaria', 'tipoAtendimentoOperadoraIntermediaria',
        # Novos campos de cabeçalho:
        'tipoTransacao', 'numeroLote', 'competenciaLote', 'dataRegistroTransacao',
        'horaRegistroTransacao', 'registroANS', 'versaoPadrao'
    ]

    # 👇 Coleta as informações do cabecalho uma vez
    cabecalho_info = {}
    cabecalho = root.find('.//ans:cabecalho', namespaces=ns)
    if cabecalho is not None:
        identificacao = cabecalho.find('ans:identificacaoTransacao', namespaces=ns)
        if identificacao is not None:
            cabecalho_info['tipoTransacao'] = identificacao.findtext('ans:tipoTransacao', default='', namespaces=ns)
            cabecalho_info['numeroLote'] = identificacao.findtext('ans:numeroLote', default='', namespaces=ns)
            cabecalho_info['competenciaLote'] = identificacao.findtext('ans:competenciaLote', default='', namespaces=ns)
            cabecalho_info['dataRegistroTransacao'] = identificacao.findtext('ans:dataRegistroTransacao', default='', namespaces=ns)
            cabecalho_info['horaRegistroTransacao'] = identificacao.findtext('ans:horaRegistroTransacao', default='', namespaces=ns)
        cabecalho_info['registroANS'] = cabecalho.findtext('ans:registroANS', default='', namespaces=ns)
        cabecalho_info['versaoPadrao'] = cabecalho.findtext('ans:versaoPadrao', default='', namespaces=ns)

    for guia in root.findall(".//ans:guiaMonitoramento", namespaces=ns):
        guia_data = {}
        # Adicionar cabecalho info a cada linha
        guia_data.update(cabecalho_info)

        for elem in guia.iter():
            tag_full = elem.tag.split('}')[-1]
            if 'data' in tag_full.lower() and elem.text:
                try:
                    date_obj = datetime.strptime(elem.text, '%Y-%m-%d')
                    guia_data[tag_full] = date_obj.strftime('%d/%m/%Y')
                except ValueError:
                    guia_data[tag_full] = elem.text
            else:
                guia_data[tag_full] = elem.text if elem.text else None

        procedimentos = guia.findall(".//ans:procedimentos", namespaces=ns)
        if procedimentos:
            for proc in procedimentos:
                proc_data = guia_data.copy()

                proc_data['codigoProcedimento'] = (proc.findtext(
                    'ans:identProcedimento/ans:Procedimento/ans:codigoProcedimento',
                    namespaces=ns
                ) or '').strip()

                proc_data['grupoProcedimento'] = (proc.findtext(
                    'ans:identProcedimento/ans:Procedimento/ans:grupoProcedimento',
                    namespaces=ns
                ) or '').strip()

                proc_data['valorInformado'] = (proc.findtext('ans:valorInformado', namespaces=ns) or '').strip()
                proc_data['valorPagoProc'] = (proc.findtext('ans:valorPagoProc', namespaces=ns) or '').strip()

                campos_procedimento = [
                    'quantidadeInformada', 'quantidadePaga',
                    'valorPagoFornecedor', 'valorCoParticipacao',
                    'unidadeMedida'
                ]
                for campo in campos_procedimento:
                    proc_data[campo] = (proc.findtext(f'ans:{campo}', namespaces=ns) or '').strip()

                proc_data['codigoTabela'] = (proc.findtext(
                    'ans:identProcedimento/ans:codigoTabela',
                    namespaces=ns
                ) or '').strip()

                all_data.append(proc_data)
        else:
            all_data.append(guia_data)

    df = pd.DataFrame(all_data)
    df['Nome da Origem'] = file.name

    date_columns = [col for col in df.columns if 'data' in col.lower()]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
        except Exception:
            pass

    colunas_existentes = [col for col in colunas_para_manter if col in df.columns]
    df = df[colunas_existentes]

    # Calcular idade
    if 'dataRealizacao' in df.columns and 'dataNascimento' in df.columns:
        def calcular_idade(row):
            try:
                data_realizacao = datetime.strptime(row['dataRealizacao'], '%d/%m/%Y')
                data_nascimento = datetime.strptime(row['dataNascimento'], '%d/%m/%Y')
                return (data_realizacao - data_nascimento).days // 365
            except Exception:
                return None
        df['Idade_na_Realização'] = df.apply(calcular_idade, axis=1)

    # Corrigir campos com zeros à esquerda para Power BI/Excel
    for col in ['numeroGuia_prestador', 'numeroGuia_operadora', 'identificacaoReembolso']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, str) and x.isdigit() else x)

    return df, content, tree


def remove_duplicate_columns(df):
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.dropna(axis=1, how='all')
    return df

@st.cache_data
def gerar_xte_do_excel(excel_file):
    ns = "http://www.ans.gov.br/padroes/tiss/schemas"

    if excel_file.name.endswith('.csv'):
        df = pd.read_csv(excel_file, dtype=str, sep=';')
    else:
        df = pd.read_excel(excel_file, dtype=str)

    def formatar_data_iso(valor):
        if pd.isna(valor):
            return ""
        if isinstance(valor, datetime):
            return valor.strftime("%Y-%m-%d")
        valor_str = str(valor).strip()
        if valor_str == "":
            return ""
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                parsed = datetime.strptime(valor_str, fmt)
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue
        try:
            serial = float(valor_str)
            base = datetime(1899, 12, 30)
            return (base + pd.to_timedelta(serial, unit="D")).strftime("%Y-%m-%d")
        except:
            return ""

    def sub(parent, tag, value, is_date=False):
        if is_date:
            value = formatar_data_iso(value)
        text = "" if pd.isna(value) else str(value).strip()
        if text:
            ET.SubElement(parent, f"ans:{tag}").text = text

    def extrair_texto(elemento):
        textos = []
        if elemento.text:
            textos.append(elemento.text)
        for filho in elemento:
            textos.extend(extrair_texto(filho))
            if filho.tail:
                textos.append(filho.tail)
        return textos

    arquivos_gerados = {}

    if "Nome da Origem" not in df.columns:
        raise ValueError("A coluna 'Nome da Origem' é obrigatória no Excel para gerar os arquivos.")

    for nome_arquivo, df_origem in df.groupby("Nome da Origem"):
        if df_origem.empty:
            continue

        agrupado = df_origem.groupby([
            "numeroGuia_prestador", "numeroGuia_operadora", "identificacaoReembolso"
        ], dropna=False)

        root = ET.Element("ans:mensagemEnvioANS", attrib={
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            "xsi:schemaLocation": f"{ns} {ns}/tissMonitoramentoV1_04_01.xsd",
            "xmlns:ans": ns
        })

        cabecalho = ET.SubElement(root, "ans:cabecalho")
        linha_cabecalho = df_origem.iloc[0]

        # data_atual = datetime.now().strftime("%Y-%m-%d")
        # hora_atual = datetime.now().strftime("%H:%M:%S")


        identificacaoTransacao = ET.SubElement(cabecalho, "ans:identificacaoTransacao")
        sub(identificacaoTransacao, "tipoTransacao", "MONITORAMENTO")

        # --- Corrigido: Pegando dados corretos da planilha
        sub(identificacaoTransacao, "numeroLote", linha_cabecalho.get("numeroLote"))
        sub(identificacaoTransacao, "competenciaLote", linha_cabecalho.get("competenciaLote"))
        sub(identificacaoTransacao, "dataRegistroTransacao", data_atual, is_date=True)
        sub(identificacaoTransacao, "horaRegistroTransacao", hora_atual)

        sub(cabecalho, "registroANS", linha_cabecalho.get("registroANS"))
        sub(cabecalho, "versaoPadrao", linha_cabecalho.get("versaoPadrao", "1.04.01"))

        mensagem = ET.SubElement(root, "ans:Mensagem")
        op_ans = ET.SubElement(mensagem, "ans:operadoraParaANS")

        for _, grupo in agrupado:
            guia = ET.SubElement(op_ans, "ans:guiaMonitoramento")
            linha = grupo.iloc[0]

            sub(guia, "tipoRegistro", linha.get("tipoRegistro"))
            sub(guia, "versaoTISSPrestador", linha.get("versaoTISSPrestador"))
            sub(guia, "formaEnvio", linha.get("formaEnvio"))

            contratado = ET.SubElement(guia, "ans:dadosContratadoExecutante")
            for tag in ["CNES", "identificadorExecutante", "codigoCNPJ_CPF", "municipioExecutante"]:
                sub(contratado, tag, linha.get(tag))

            beneficiario = ET.SubElement(guia, "ans:dadosBeneficiario")
            ident = ET.SubElement(beneficiario, "ans:identBeneficiario")
            sub(ident, "numeroCartaoNacionalSaude", linha.get("numeroCartaoNacionalSaude"))
            sub(ident, "cpfBeneficiario", linha.get("cpfBeneficiario"))

            sexo = str(linha.get("sexo")).strip()
            if sexo not in ["1", "3"]:
                sexo = "1"
            sub(ident, "sexo", sexo)

            sub(ident, "dataNascimento", linha.get("dataNascimento"), is_date=True)
            sub(ident, "municipioResidencia", linha.get("municipioResidencia"))
            sub(beneficiario, "numeroRegistroPlano", linha.get("numeroRegistroPlano"))

            for tag in ["tipoEventoAtencao", "origemEventoAtencao", "numeroGuia_prestador",
                        "numeroGuia_operadora", "identificacaoReembolso"]:
                sub(guia, tag, linha.get(tag))

            formas = ET.SubElement(guia, "ans:formasRemuneracao")
            sub(formas, "formaRemuneracao", linha.get("formaRemuneracao"))
            sub(formas, "valorRemuneracao", linha.get("valorRemuneracao"))

            for tag in ["dataAutorizacao", "dataRealizacao", "dataProtocoloCobranca", "dataPagamento",
                        "dataProcessamentoGuia", "indicacaoRecemNato", "indicacaoAcidente",
                        "caraterAtendimento", "tipoInternacao", "regimeInternacao",
                        "tipoFaturamento", "motivoSaida"]:
                if tag in linha:
                    sub(guia, tag, linha.get(tag), is_date=("data" in tag.lower()))

            valores = ET.SubElement(guia, "ans:valoresGuia")
            for tag in ["valorTotalInformado", "valorProcessado", "valorTotalPagoProcedimentos",
                        "valorTotalDiarias", "valorTotalTaxas", "valorTotalMateriais",
                        "valorTotalOPME", "valorTotalMedicamentos", "valorGlosaGuia",
                        "valorPagoGuia", "valorPagoFornecedores", "valorTotalTabelaPropria",
                        "valorTotalCoParticipacao"]:
                sub(valores, tag, linha.get(tag))

            for _, proc in grupo.iterrows():
                procedimento = ET.SubElement(guia, "ans:procedimentos")
                ident_proc = ET.SubElement(procedimento, "ans:identProcedimento")
                sub(ident_proc, "codigoTabela", proc.get("codigoTabela"))
                proc_node = ET.SubElement(ident_proc, "ans:Procedimento")
                if proc.get("grupoProcedimento"):
                    sub(proc_node, "grupoProcedimento", proc.get("grupoProcedimento"))
                sub(proc_node, "codigoProcedimento", proc.get("codigoProcedimento"))

                for tag in ["quantidadeInformada", "valorInformado", "quantidadePaga", "unidadeMedida",
                            "valorPagoProc", "valorPagoFornecedor", "valorCoParticipacao"]:
                    sub(procedimento, tag, proc.get(tag))

        conteudo = ''.join(extrair_texto(cabecalho) + extrair_texto(mensagem))
        hash_value = hashlib.md5(conteudo.encode('iso-8859-1')).hexdigest()

        epilogo = ET.SubElement(root, "ans:epilogo")
        ET.SubElement(epilogo, "ans:hash").text = hash_value

        final_xml = ET.tostring(root, encoding="utf-8")
        final_pretty = minidom.parseString(final_xml).toprettyxml(indent="  ", encoding="iso-8859-1")

        nome_base, _ = os.path.splitext(nome_arquivo)
        nome_limpo = re.sub(r'[^a-zA-Z0-9_\-]', '_', nome_base)
        arquivos_gerados[f"{nome_limpo}.xml"] = final_pretty
        arquivos_gerados[f"{nome_limpo}.xte"] = final_pretty

    return arquivos_gerados

######################################### STREAM LIT #########################################  


# Forçar tema escuro
st.set_page_config(page_title="Conversor Avançado de XTE", layout="wide")

# Custom CSS para destaque do menu
st.markdown("""
    <style>
        section[data-testid="stSidebar"] .css-ng1t4o {
            background-color: #1e1e1e;
            color: white;
            font-weight: bold;
            font-size: 1.1rem;
        }
        section[data-testid="stSidebar"] label {
            color: white !important;
        }
    </style>
""", unsafe_allow_html=True)

st.sidebar.title("AM Consultoria")
menu = st.sidebar.radio("Escolha uma operação:", [
    "Converter XTE para Excel e CSV",
    "Converter Excel para XTE/XML"
])

st.title("Conversor Avançado de XTE ⇄ Excel")

if menu == "Converter XTE para Excel e CSV":
    st.subheader("📄➡📊 Transformar arquivos .XTE em Excel e CSV")
    
    st.markdown("""
    Este modo permite que você envie **dois ou mais arquivos `.xte`** e receba:

    - Um **arquivo Excel (.xlsx)** consolidado.
    - Um **arquivo CSV (.csv)** com os mesmos dados.

    Ideal para visualizar, editar e analisar seus dados fora do sistema.
    """)

    uploaded_files = st.file_uploader("Selecione os arquivos .xte", accept_multiple_files=True, type=["xte"])

    if uploaded_files:
        st.info(f"Você enviou {len(uploaded_files)} arquivos. Aguarde enquanto processamos.")
        progress_bar = st.progress(0)
        status_text = st.empty()
        all_dfs = []

        total = len(uploaded_files)
        start_time = time.time()

        for i, file in enumerate(uploaded_files):
            step_start = time.time()
            with st.spinner(f"Lendo arquivo {file.name}..."):
                df, _, _ = parse_xte(file)
                df['Nome da Origem'] = file.name
                all_dfs.append(df)

            elapsed = time.time() - start_time
            avg_time = elapsed / (i + 1)
            est_remaining = avg_time * (total - (i + 1))

            percent_complete = (i + 1) / total
            progress_bar.progress(percent_complete)

            status_text.markdown(
                f"Processado {i + 1} de {total} arquivos ({percent_complete:.0%})  \
                Estimado restante: {int(est_remaining)} segundos 🕒"
            )

        final_df = pd.concat(all_dfs, ignore_index=True)
        st.success(f"✅ Processamento concluído: {len(final_df)} registros.")

        st.subheader("🔍 Pré-visualização dos dados:")
        st.dataframe(final_df.head(20))

        excel_buffer = io.BytesIO()
        final_df.to_excel(excel_buffer, index=False)

        csv_buffer = io.StringIO()
        final_df.to_csv(csv_buffer, index=False, sep=";", encoding="utf-8", float_format='%.2f')

        st.download_button("⬇ Baixar Excel Consolidado", data=excel_buffer.getvalue(), file_name="dados_consolidados.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.download_button("⬇ Baixar CSV Consolidado", data=csv_buffer.getvalue(), file_name="dados_consolidados.csv", mime="text/csv")

elif menu == "Converter Excel para XTE/XML":
    st.subheader("📊➡📄 Transformar Excel em arquivos .XTE/XML")

    st.markdown("""
    Aqui você pode carregar **um arquivo Excel atualizado** e o sistema irá:

    - Processar os dados.
    - Gerar **vários arquivos `.xte` ou `.xml`**.
    - Compactar os arquivos `.xml` automaticamente.
    - Permitir que você baixe os arquivos `.xte` somente quando desejar.

    **Antes disso**, você poderá baixar **um exemplo do primeiro arquivo gerado.**
    """)

    excel_file = st.file_uploader("Selecione o arquivo Excel (.xlsx ou .csv)", type=["xlsx", "csv"])

    if excel_file:
        st.info("🔄 Processando o arquivo...")

        import pytz
        fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')
        datetime_brasil = datetime.now(fuso_horario_brasil)
        data_atual = datetime_brasil.strftime("%Y-%m-%d")
        hora_atual = datetime_brasil.strftime("%H:%M:%S")
            
        # Informar ao usuário a data/hora que será usada
        st.info(f"📅 Data e hora da geração: {datetime_brasil.strftime('%d/%m/%Y %H:%M:%S')} (Horário de Brasília)")
        try:
            with st.spinner("Gerando arquivos..."):
                updated_files = gerar_xte_do_excel(excel_file)

            # Separar XMLs e XTEs
            xml_files = {k: v for k, v in updated_files.items() if k.endswith(".xml")}
            xte_files = {k.replace(".xml", ".xte"): v for k, v in updated_files.items() if k.endswith(".xml")}

            # Exemplo de preview
            first_key = next(iter(xml_files))
            first_file = xml_files[first_key]

            st.download_button(
                f"⬇ Baixar exemplo: {first_key}",
                data=first_file,
                file_name=first_key,
                mime="application/xml"
            )

            st.download_button(
                f"⬇ Baixar exemplo em XTE: {first_key.replace('.xml', '.xte')}",
                data=first_file,
                file_name=first_key.replace('.xml', '.xte'),
                mime="application/xml"
            )

            # Compactar XMLs automaticamente
            st.info("📦 Compactando arquivos XML...")
            xml_zip_buffer = io.BytesIO()
            start_time = time.time()
            progress = st.progress(0)
            status = st.empty()
            total = len(xml_files)

            with zipfile.ZipFile(xml_zip_buffer, "w") as zipf:
                for i, (filename, content) in enumerate(xml_files.items()):
                    zipf.writestr(filename, content)
                    elapsed = time.time() - start_time
                    avg = elapsed / (i + 1)
                    remaining = avg * (total - (i + 1))
                    progress.progress((i + 1) / total)
                    status.markdown(f"📄 Adicionando {i + 1}/{total} arquivos XML - ⏳ Restante: {int(remaining)}s")

            st.success("✅ Arquivo ZIP com XMLs pronto!")
            st.download_button(
                "⬇ Baixar ZIP de XMLs",
                data=xml_zip_buffer.getvalue(),
                file_name="arquivos_xml.zip",
                mime="application/zip"
            )

            # Botão para gerar e baixar XTEs
            if st.button("📁 Gerar e Baixar Arquivo ZIP com XTEs"):
                st.info("📦 Compactando arquivos XTE...")
                xte_zip_buffer = io.BytesIO()
                start_time = time.time()
                progress_xte = st.progress(0)
                status_xte = st.empty()
                total_xte = len(xte_files)

                with zipfile.ZipFile(xte_zip_buffer, "w") as zipf:
                    for i, (filename, content) in enumerate(xte_files.items()):
                        zipf.writestr(filename, content)
                        elapsed = time.time() - start_time
                        avg = elapsed / (i + 1)
                        remaining = avg * (total_xte - (i + 1))
                        progress_xte.progress((i + 1) / total_xte)
                        status_xte.markdown(f"📄 Adicionando {i + 1}/{total_xte} arquivos XTE - ⏳ Restante: {int(remaining)}s")

                st.success("✅ Arquivo ZIP com XTEs pronto!")
                st.download_button(
                    "⬇ Baixar ZIP de XTEs",
                    data=xte_zip_buffer.getvalue(),
                    file_name="arquivos_xte.zip",
                    mime="application/zip"
                )

        except Exception as e:
            st.error(f"Erro durante o processamento: {str(e)}")
            st.error("Verifique se o arquivo Excel possui a estrutura correta.")
