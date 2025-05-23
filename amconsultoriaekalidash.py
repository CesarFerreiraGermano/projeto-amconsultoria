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
                # ADICIONE ISSO APÓS OS OUTROS CAMPOS DO PROCEDIMENTO:
                proc_data['registroANSOperadoraIntermediaria'] = (proc.findtext('ans:registroANSOperadoraIntermediaria', namespaces=ns) or '').strip()
                proc_data['tipoAtendimentoOperadoraIntermediaria'] = (proc.findtext('ans:tipoAtendimentoOperadoraIntermediaria', namespaces=ns) or '').strip()

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

    # Obtém data/hora ATUAL no momento da geração
    data_atual = datetime.now().strftime("%Y-%m-%d")
    hora_atual = datetime.now().strftime("%H:%M:%S")

    if hasattr(excel_file, 'name') and excel_file.name.endswith('.csv'): # Checa se tem o atributo 'name'
        df = pd.read_csv(excel_file, dtype=str, sep=';')
    else:
        df = pd.read_excel(excel_file, dtype=str)

    def formatar_data_iso(valor):
        if pd.isna(valor):
            return ""
        if isinstance(valor, datetime): # Se já for datetime (improvável do Excel como str)
            return valor.strftime("%Y-%m-%d")
        valor_str = str(valor).strip()
        if valor_str == "":
            return ""
        
        # Tenta formatos comuns de data
        for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed_datetime = datetime.strptime(valor_str, fmt)
                return parsed_datetime.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        # Tenta converter de número serial do Excel se for um número
        try:
            # Verifica se é um número (pode ser float com .0)
            if valor_str.replace('.', '', 1).isdigit():
                serial = float(valor_str)
                # A data base do Excel é 30/12/1899 para números seriais
                base_date = datetime(1899, 12, 30)
                delta = pd.to_timedelta(serial, unit='D')
                return (base_date + delta).strftime("%Y-%m-%d")
        except ValueError:
            pass # Não é um número serial válido ou float simples

        return valor_str # Retorna o valor original se não conseguir parsear como data conhecida


    def sub(parent, tag, value, is_date=False):
        if is_date:
            value = formatar_data_iso(value)
        
        text = "" if pd.isna(value) else str(value).strip()
        
        # Adiciona o elemento apenas se o texto não estiver vazio OU se a tag for obrigatória (lógica não implementada aqui)
        # Para simplificar, vamos adicionar se text não for vazio. Se alguma tag vazia for obrigatória pelo schema,
        # esta lógica pode precisar de ajuste para enviar tags vazias (ex: <ans:tag></ans:tag>)
        if text: 
            ET.SubElement(parent, f"ans:{tag}").text = text

    def extrair_texto(elemento):
        textos = []
        if elemento.text:
            textos.append(elemento.text.strip()) # Adicionado strip() aqui também
        for filho in elemento:
            textos.extend(extrair_texto(filho))
            if filho.tail:
                textos.append(filho.tail.strip()) # Adicionado strip() aqui também
        return textos

    arquivos_gerados = {}

    if "Nome da Origem" not in df.columns:
        raise ValueError("A coluna 'Nome da Origem' é obrigatória no Excel para gerar os arquivos.")

    for nome_arquivo, df_origem in df.groupby("Nome da Origem"):
        if df_origem.empty:
            continue

        agrupado = df_origem.groupby([
            "numeroGuia_prestador", "numeroGuia_operadora", "identificacaoReembolso"
        ], dropna=False) # dropna=False é importante para guias sem esses números

        root = ET.Element("ans:mensagemEnvioANS", attrib={
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            "xsi:schemaLocation": f"{ns} {ns}/tissMonitoramentoV1_04_01.xsd", # Hardcoded para a versão correta
            "xmlns:ans": ns
        })

        cabecalho = ET.SubElement(root, "ans:cabecalho")
        linha_cabecalho = df_origem.iloc[0] # Pega a primeira linha para dados do cabeçalho do lote/arquivo

        identificacaoTransacao = ET.SubElement(cabecalho, "ans:identificacaoTransacao")
        sub(identificacaoTransacao, "tipoTransacao", "MONITORAMENTO") # Conforme TISS Monitoramento
        sub(identificacaoTransacao, "numeroLote", linha_cabecalho.get("numeroLote"))
        sub(identificacaoTransacao, "competenciaLote", linha_cabecalho.get("competenciaLote"))
        sub(identificacaoTransacao, "dataRegistroTransacao", data_atual)  # Usa data atual da geração
        sub(identificacaoTransacao, "horaRegistroTransacao", hora_atual)  # Usa hora atual da geração

        sub(cabecalho, "registroANS", linha_cabecalho.get("registroANS"))
        sub(cabecalho, "versaoPadrao", linha_cabecalho.get("versaoPadrao", "1.04.01")) # Default para a versão do schema

        mensagem = ET.SubElement(root, "ans:Mensagem")
        op_ans = ET.SubElement(mensagem, "ans:operadoraParaANS")

        for _, grupo_guia_key in agrupado: # Iterando sobre cada grupo que representa uma guia
            guia = ET.SubElement(op_ans, "ans:guiaMonitoramento")
            # linha_guia representa os dados principais da guia (primeira linha do agrupamento)
            linha_guia = grupo_guia_key.iloc[0]

            # Sequência de acordo com ct_monitoramentoGuia do XSD tissMonitoramentoV1_04_01.xsd
            sub(guia, "tipoRegistro", linha_guia.get("tipoRegistro"))
            sub(guia, "versaoTISSPrestador", linha_guia.get("versaoTISSPrestador"))
            sub(guia, "formaEnvio", linha_guia.get("formaEnvio"))

            dadosContratadoExecutante_el = ET.SubElement(guia, "ans:dadosContratadoExecutante")
            sub(dadosContratadoExecutante_el, "CNES", linha_guia.get("CNES"))
            sub(dadosContratadoExecutante_el, "identificadorExecutante", linha_guia.get("identificadorExecutante"))
            sub(dadosContratadoExecutante_el, "codigoCNPJ_CPF", linha_guia.get("codigoCNPJ_CPF"))
            sub(dadosContratadoExecutante_el, "municipioExecutante", linha_guia.get("municipioExecutante"))

            sub(guia, "registroANSOperadoraIntermediaria", linha_guia.get("registroANSOperadoraIntermediaria"))
            sub(guia, "tipoAtendimentoOperadoraIntermediaria", linha_guia.get("tipoAtendimentoOperadoraIntermediaria"))

            dadosBeneficiario_el = ET.SubElement(guia, "ans:dadosBeneficiario")
            identBeneficiario_el = ET.SubElement(dadosBeneficiario_el, "ans:identBeneficiario")
            sub(identBeneficiario_el, "numeroCartaoNacionalSaude", linha_guia.get("numeroCartaoNacionalSaude"))
            sub(identBeneficiario_el, "cpfBeneficiario", linha_guia.get("cpfBeneficiario"))
            sexo_val = str(linha_guia.get("sexo", "")).strip()
            if sexo_val not in ["1", "3"] and sexo_val: # Se preenchido e inválido, TISS pode rejeitar. Ajuste conforme regra de negócio.
                 sexo_val = "" # Ou um valor padrão, ou deixar em branco se o campo for opcional e puder ser vazio.
            if sexo_val: # Só adiciona se tiver valor (1 ou 3)
                 sub(identBeneficiario_el, "sexo", sexo_val)
            sub(identBeneficiario_el, "dataNascimento", linha_guia.get("dataNascimento"), is_date=True)
            sub(identBeneficiario_el, "municipioResidencia", linha_guia.get("municipioResidencia"))
            sub(dadosBeneficiario_el, "numeroRegistroPlano", linha_guia.get("numeroRegistroPlano"))

            sub(guia, "tipoEventoAtencao", linha_guia.get("tipoEventoAtencao"))
            sub(guia, "origemEventoAtencao", linha_guia.get("origemEventoAtencao"))
            sub(guia, "numeroGuia_prestador", linha_guia.get("numeroGuia_prestador"))
            sub(guia, "numeroGuia_operadora", linha_guia.get("numeroGuia_operadora"))
            sub(guia, "identificacaoReembolso", linha_guia.get("identificacaoReembolso"))
            sub(guia, "identificacaoValorPreestabelecido", linha_guia.get("identificacaoValorPreestabelecido"))

            # formasRemuneracao (maxOccurs="unbounded") - Adapte se houver múltiplas no Excel para a mesma guia
            if pd.notna(linha_guia.get("formaRemuneracao")) or pd.notna(linha_guia.get("valorRemuneracao")):
                formasRemuneracao_el = ET.SubElement(guia, "ans:formasRemuneracao")
                sub(formasRemuneracao_el, "formaRemuneracao", linha_guia.get("formaRemuneracao"))
                sub(formasRemuneracao_el, "valorRemuneracao", linha_guia.get("valorRemuneracao"))
            
            sub(guia, "guiaSolicitacaoInternacao", linha_guia.get("guiaSolicitacaoInternacao"))
            sub(guia, "dataSolicitacao", linha_guia.get("dataSolicitacao"), is_date=True)
            sub(guia, "numeroGuiaSPSADTPrincipal", linha_guia.get("numeroGuiaSPSADTPrincipal"))
            sub(guia, "dataAutorizacao", linha_guia.get("dataAutorizacao"), is_date=True)
            sub(guia, "dataRealizacao", linha_guia.get("dataRealizacao"), is_date=True)
            sub(guia, "dataInicialFaturamento", linha_guia.get("dataInicialFaturamento"), is_date=True)
            sub(guia, "dataFimPeriodo", linha_guia.get("dataFimPeriodo"), is_date=True)
            sub(guia, "dataProtocoloCobranca", linha_guia.get("dataProtocoloCobranca"), is_date=True)
            sub(guia, "dataPagamento", linha_guia.get("dataPagamento"), is_date=True)
            sub(guia, "dataProcessamentoGuia", linha_guia.get("dataProcessamentoGuia"), is_date=True)
            
            sub(guia, "tipoConsulta", linha_guia.get("tipoConsulta"))
            sub(guia, "cboExecutante", linha_guia.get("cboExecutante"))
            sub(guia, "indicacaoRecemNato", linha_guia.get("indicacaoRecemNato"))
            sub(guia, "indicacaoAcidente", linha_guia.get("indicacaoAcidente"))
            sub(guia, "caraterAtendimento", linha_guia.get("caraterAtendimento"))
            sub(guia, "tipoInternacao", linha_guia.get("tipoInternacao"))
            sub(guia, "regimeInternacao", linha_guia.get("regimeInternacao"))

            # diagnosticosCID10 (contém diagnosticoCID maxOccurs="4")
            # Adapte se houver múltiplas colunas CID (ex: diagnosticoCID1, diagnosticoCID2) no Excel
            cid_principal = linha_guia.get("diagnosticoCID") # Ou o nome da sua coluna principal de CID
            if pd.notna(cid_principal):
                diagnosticosCID10_el = ET.SubElement(guia, "ans:diagnosticosCID10")
                sub(diagnosticosCID10_el, "diagnosticoCID", cid_principal)
                # Exemplo para CIDs adicionais, se existirem colunas:
                # for i in range(2, 5): # Para diagnosticoCID2, diagnosticoCID3, diagnosticoCID4
                #     cid_adicional = linha_guia.get(f"diagnosticoCID{i}")
                #     if pd.notna(cid_adicional):
                #         sub(diagnosticosCID10_el, "diagnosticoCID", cid_adicional)
            
            sub(guia, "tipoAtendimento", linha_guia.get("tipoAtendimento"))
            sub(guia, "regimeAtendimento", linha_guia.get("regimeAtendimento"))
            sub(guia, "saudeOcupacional", linha_guia.get("saudeOcupacional"))
            sub(guia, "tipoFaturamento", linha_guia.get("tipoFaturamento"))
            sub(guia, "diariasAcompanhante", linha_guia.get("diariasAcompanhante"))
            sub(guia, "diariasUTI", linha_guia.get("diariasUTI"))
            sub(guia, "motivoSaida", linha_guia.get("motivoSaida"))

            valoresGuia_el = ET.SubElement(guia, "ans:valoresGuia")
            tags_valores_guia = [
                "valorTotalInformado", "valorProcessado", "valorTotalPagoProcedimentos",
                "valorTotalDiarias", "valorTotalTaxas", "valorTotalMateriais",
                "valorTotalOPME", "valorTotalMedicamentos", "valorGlosaGuia",
                "valorPagoGuia", "valorPagoFornecedores", "valorTotalTabelaPropria",
                "valorTotalCoParticipacao"
            ]
            for tag_vg in tags_valores_guia:
                sub(valoresGuia_el, tag_vg, linha_guia.get(tag_vg))

            # declaracaoNascido (maxOccurs="8") - Adapte para múltiplas ocorrências
            sub(guia, "declaracaoNascido", linha_guia.get("declaracaoNascido"))
            # declaracaoObito (maxOccurs="8") - Adapte para múltiplas ocorrências
            sub(guia, "declaracaoObito", linha_guia.get("declaracaoObito"))

            # Loop para os procedimentos da guia
            for _, proc_linha in grupo_guia_key.iterrows(): # Itera sobre todas as linhas do grupo (cada linha é um procedimento)
                procedimentos_el = ET.SubElement(guia, "ans:procedimentos")
                
                identProcedimento_el = ET.SubElement(procedimentos_el, "ans:identProcedimento")
                sub(identProcedimento_el, "codigoTabela", proc_linha.get("codigoTabela"))
                Procedimento_el = ET.SubElement(identProcedimento_el, "ans:Procedimento")
                # No XSD é uma choice: grupoProcedimento OU codigoProcedimento. Assumindo que ambos podem estar no Excel
                # e a lógica do 'sub' adicionará o que estiver presente. Se só um é permitido, ajuste.
                if pd.notna(proc_linha.get("grupoProcedimento")):
                    sub(Procedimento_el, "grupoProcedimento", proc_linha.get("grupoProcedimento"))
                else: # Garante que ou grupo ou código seja enviado se um deles existir
                    sub(Procedimento_el, "codigoProcedimento", proc_linha.get("codigoProcedimento"))
                
                # denteRegiao (complex choice) e denteFace - Adicionar lógica se usar odontologia
                # Exemplo:
                # if pd.notna(proc_linha.get("codDente")) or pd.notna(proc_linha.get("codRegiao")):
                #    denteRegiao_el = ET.SubElement(procedimentos_el, "ans:denteRegiao")
                #    if pd.notna(proc_linha.get("codDente")):
                #        sub(denteRegiao_el, "codDente", proc_linha.get("codDente"))
                #    else:
                #        sub(denteRegiao_el, "codRegiao", proc_linha.get("codRegiao"))
                # sub(procedimentos_el, "denteFace", proc_linha.get("denteFace"))

                sub(procedimentos_el, "quantidadeInformada", proc_linha.get("quantidadeInformada"))
                sub(procedimentos_el, "valorInformado", proc_linha.get("valorInformado"))
                sub(procedimentos_el, "quantidadePaga", proc_linha.get("quantidadePaga"))
                sub(procedimentos_el, "unidadeMedida", proc_linha.get("unidadeMedida"))
                sub(procedimentos_el, "valorPagoProc", proc_linha.get("valorPagoProc"))
                sub(procedimentos_el, "valorPagoFornecedor", proc_linha.get("valorPagoFornecedor"))
                sub(procedimentos_el, "CNPJFornecedor", proc_linha.get("CNPJFornecedor")) # Adicionado conforme XSD
                sub(procedimentos_el, "valorCoParticipacao", proc_linha.get("valorCoParticipacao"))
                
                # detalhePacote (maxOccurs="unbounded") - Adicionar lógica se usar pacotes
                
                # Campos de Operadora Intermediária DENTRO de cada procedimento (conforme seu script V2)
                sub(procedimentos_el, "registroANSOperadoraIntermediaria", proc_linha.get("registroANSOperadoraIntermediaria"))
                sub(procedimentos_el, "tipoAtendimentoOperadoraIntermediaria", proc_linha.get("tipoAtendimentoOperadoraIntermediaria"))

        # Hash e Epílogo (fora do loop de guias, uma vez por arquivo)
        # É importante que extrair_texto seja chamado APÓS todo o conteúdo de <cabecalho> e <Mensagem> ser construído
        # Limpa qualquer texto ou cauda do elemento root antes de adicionar o epílogo, se necessário
        root.text = None 
        root.tail = None
        
        # A função extrair_texto precisa ser robusta para não pegar texto fora de cabecalho e mensagem
        # Vamos assumir que ela pega apenas o conteúdo textual desses dois filhos diretos de root
        conteudo_cabecalho = ''.join(extrair_texto(cabecalho))
        conteudo_mensagem = ''.join(extrair_texto(mensagem))
        conteudo_para_hash = conteudo_cabecalho + conteudo_mensagem
        
        hash_value = hashlib.md5(conteudo_para_hash.encode('iso-8859-1')).hexdigest()

        epilogo = ET.SubElement(root, "ans:epilogo")
        ET.SubElement(epilogo, "ans:hash").text = hash_value

        # Prettify XML
        # ET.indent(root) # Para Python 3.9+
        xml_string = ET.tostring(root, encoding="utf-8", method="xml")
        dom = minidom.parseString(xml_string)
        final_pretty = dom.toprettyxml(indent="  ", encoding="iso-8859-1")
        
        # Limpa nome do arquivo para evitar caracteres inválidos
        nome_base, _ = os.path.splitext(nome_arquivo)
        nome_limpo = re.sub(r'[^a-zA-Z0-9_\-]', '_', nome_base) # Garante nome de arquivo válido
        
        arquivos_gerados[f"{nome_limpo}.xml"] = final_pretty
        arquivos_gerados[f"{nome_limpo}.xte"] = final_pretty # XTE e XML com mesmo conteúdo

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
