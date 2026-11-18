import json
import os
from copy import deepcopy
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from src.infrastructure.infobip.client import InfobipClient
from src.infrastructure.logging.logger import get_logger
from src.repository.envio_repository import EnvioRepository
from src.shared.utils.uuid_gerenator import build_custom_message_id, ensure_unique_custom_message_id
from src.shared.utils.template_renderer import render_template
from src.shared.utils.email_sender import Email


class EnvioService:
    STATUSS_LIBERADOS = {"PENDENTE__APROVADO", "APROVADO__GERENCIA"}
    IDS_CEDENTE_LIMITE_ESPECIAL = (3, 51)
    CEDENTE_LIMITE_320 = "DELTA - CONSIGNADO"
    CANAL_LIMITE_320 = "PREVENTIVO"
    CEDENTE_LIMITE_TOTAL = "TOTAL"
    STATUS_DESC_CEDENTE_RESERVADO = "Bloqueado por cedente reservado: TOTAL"

    def __init__(self):
        self.logger = get_logger("envio")
        self.client = InfobipClient(logger=self.logger)
        self.max_workers = 5
        self.limite_cedente_hora = 25000
        self.limite_global_hora = 100000
        self.limite_pendentes_cedente = 25000
        self.limite_cedente_hora_especial = 150000
        self.limite_global_hora_especial = 200000
        self.limite_pendentes_cedente_especial = 150000
        self.limite_cedente_mes_ativo = True
        self.limite_cedente_mes_incluir_aprovado = True
        self.bloquear_fora_padrao_mes_incluir_aprovado = True
        self.limite_cedente_semana_ativo = True
        self.limite_cedente_semana_incluir_aprovado = False
        self.bloquear_fora_padrao_semana_incluir_aprovado = True
        self.max_envios_execucao = 10000
        self.batch_envio_tamanho = 500
        self.update_batch_tamanho = 500
        self.spool_dir = Path("spool") / "infobip_envio_pending"
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self.reserva_spool_dir = Path("spool") / "infobip_reserva_pending"
        self.reserva_spool_dir.mkdir(parents=True, exist_ok=True)
        self.modo_teste_sem_envio = False
        # Quando ativo, envia apenas o canal definido em `canal_envio_permitido`.
        self.filtro_canal_ativo = False
        self.canal_envio_permitido = "PREVENTIVO"
        self._alerta_hora_enviado = set()

    def processar_envios(self):
        lock_conn = EnvioRepository.adquirir_lock_envio(logger=self.logger)
        ids_reserva = []
        reserva_file = None
        if not lock_conn:
            self.logger.warning("Processo de envio abortado: lock em uso")
            return
        try:
            self._reconciliar_spool_envio()
            self._reconciliar_reservas_pendentes()
            self._aplicar_bloqueios_iniciais()

            total_ultima_hora = EnvioRepository.contar_envios_ultima_hora_total()
            canal_filtro = self.canal_envio_permitido if self.filtro_canal_ativo else None
            limite_global_hora = self._obter_limite_global_hora(canal=canal_filtro)
            bloquear_pendentes_global = total_ultima_hora >= limite_global_hora
            if bloquear_pendentes_global:
                self.logger.warning(
                    "Limite global por hora atingido (total_ultima_hora=%s). Sem envio ate virar a hora.",
                    total_ultima_hora
                )
                self._enviar_alerta_hora()
                return

            limite = self.max_envios_execucao if self.max_envios_execucao > 0 else None
            pendentes = []

            pendentes_aprovados = EnvioRepository.buscar_pendentes_aprovados(
                limit=limite,
                logger=self.logger
            )
            if pendentes_aprovados:
                pendentes.extend(pendentes_aprovados)

            restante = None
            if limite is not None:
                restante = max(0, limite - len(pendentes))

            if restante is None or restante > 0:
                pendentes_normais = EnvioRepository.buscar_pendentes_para_envio(
                    limit=restante,
                    limite_pendentes_cedente=self.limite_cedente_hora,
                    limite_pendentes_fila=self.limite_pendentes_cedente,
                    ids_cedente_limite_especial=self.IDS_CEDENTE_LIMITE_ESPECIAL,
                    limite_pendentes_cedente_especial=self.limite_cedente_hora_especial,
                    limite_pendentes_fila_especial=self.limite_pendentes_cedente_especial,
                    permitir_pendentes=not bloquear_pendentes_global,
                    canal=canal_filtro,
                    logger=self.logger
                )
                if pendentes_normais:
                    pendentes.extend(pendentes_normais)

            if self.limite_cedente_semana_ativo:
                pendentes = self._aplicar_reserva_limite_semanal(pendentes)

            if self.limite_cedente_mes_ativo:
                pendentes = self._aplicar_reserva_limite_mensal(pendentes)

            if not pendentes:
                self.logger.info("Pendentes: 0")
                self._enviar_alerta_hora()
                return

            ids_reserva = [getattr(r, "ID", None) for r in pendentes if getattr(r, "ID", None) is not None]
            EnvioRepository.marcar_reservado_batch(
                ids_reserva,
                batch_size=self.update_batch_tamanho,
                logger=self.logger
            )
            reserva_file = self._persistir_reserva_execucao(ids_reserva)

            self.logger.info("Pendentes carregados na fila: %s", len(pendentes))
            self._log_pendentes_por_status(pendentes)

            fila_envio = []
            updates_buffer = []
            total_preparados = 0

            for registro in pendentes:
                preparado, update = self._preparar_registro(registro)
                if update:
                    updates_buffer.append(update)
                    if len(updates_buffer) >= self.update_batch_tamanho:
                        self._flush_updates(updates_buffer)
                        updates_buffer.clear()
                if preparado:
                    fila_envio.append(preparado)
                    total_preparados += 1

            if updates_buffer:
                self._flush_updates(updates_buffer)
                updates_buffer.clear()

            if not fila_envio:
                self.logger.info("Fila vazia apos validacoes")
                self._enviar_alerta_hora()
                return

            total_enviados_execucao = 0
            total_erros_execucao = 0

            batches = [
                fila_envio[i:i + self.batch_envio_tamanho]
                for i in range(0, len(fila_envio), self.batch_envio_tamanho)
            ]

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._enviar_batch, batch) for batch in batches]
                updates_buffer = []
                for future in as_completed(futures):
                    try:
                        updates = future.result()
                        if updates:
                            for upd in updates:
                                if upd.get("status") == "ENVIADO":
                                    total_enviados_execucao += 1
                                elif upd.get("status") == "ERRO":
                                    total_erros_execucao += 1
                            updates_buffer.extend(updates)
                            if len(updates_buffer) >= self.update_batch_tamanho:
                                self._flush_updates(updates_buffer)
                                updates_buffer.clear()
                    except Exception as e:
                        self.logger.exception("Excecao thread envio batch: %s", str(e))

                if updates_buffer:
                    self._flush_updates(updates_buffer)
                    updates_buffer.clear()

            self.logger.info(
                "Total enviado na execucao: %s | erros=%s | preparados=%s",
                total_enviados_execucao,
                total_erros_execucao,
                total_preparados
            )
            self._enviar_alerta_hora()
        except Exception as e:
            self.logger.exception("Falha no processo de envio: %s", str(e))
            raise
        finally:
            try:
                rows = EnvioRepository.marcar_nao_enviado_reservados_batch(
                    ids_reserva,
                    status_desc="Nao enviado: processo interrompido antes da confirmacao de envio",
                    batch_size=self.update_batch_tamanho,
                    logger=self.logger
                )
                if rows:
                    self.logger.warning(
                        "Reservados remanescentes marcados como NAO_ENVIADO ao finalizar execucao: %s",
                        rows
                    )
            except Exception:
                self.logger.exception("Falha ao limpar registros RESERVADO remanescentes")
            else:
                self._remover_reserva_execucao(reserva_file)
            EnvioRepository.liberar_lock_envio(lock_conn, logger=self.logger)

    def _log_pendentes_por_status(self, pendentes):
        totais = {}
        for row in pendentes:
            status = str(getattr(row, "STATUS", "SEM_STATUS") or "SEM_STATUS").upper()
            totais[status] = totais.get(status, 0) + 1

        if not totais:
            return

        resumo = " | ".join(f"{status}={totais[status]}" for status in sorted(totais.keys()))
        self.logger.info("Pendentes por status: %s", resumo)

    def _aplicar_reserva_limite_mensal(self, pendentes):
        if not pendentes:
            return pendentes

        resumo = EnvioRepository.buscar_resumo_mensal_limite_cedente()

        def _normalizar_cedente(valor):
            return str(valor or "").strip().upper()

        restante_por_cedente = {}
        for row in resumo:
            cedente = _normalizar_cedente(getattr(row, "CEDENTE", None))
            limite = int(getattr(row, "LIMITE_MENSAL", 0) or 0)
            total_enviado = int(getattr(row, "TOTAL_ENVIADO", 0) or 0)
            restante_por_cedente[cedente] = max(0, limite - total_enviado)

        consumido_lote_por_cedente = {}
        restante_total = restante_por_cedente.get(self.CEDENTE_LIMITE_TOTAL)
        consumido_lote_total = 0
        pendentes_liberados = []
        updates_bloqueio = []

        for registro in pendentes:
            cedente = _normalizar_cedente(getattr(registro, "CEDENTE", None))
            id_registro = getattr(registro, "ID", None)
            status_registro = str(getattr(registro, "STATUS", "") or "").upper()

            # APROVADO__GERENCIA ignora o limite mensal.
            # PENDENTE__APROVADO deve respeitar o limite mensal.
            if status_registro == "APROVADO__GERENCIA":
                pendentes_liberados.append(registro)
                continue

            if cedente not in restante_por_cedente:
                if id_registro is not None:
                    updates_bloqueio.append(
                        self._build_update(
                            id_registro=id_registro,
                            status="PENDENTE_BLOQUEADO",
                            status_desc="Bloqueado por cedente fora do padrao mensal",
                        )
                    )
                continue

            restante_global = None if restante_total is None else restante_total - consumido_lote_total
            restante_cedente = restante_por_cedente[cedente] - consumido_lote_por_cedente.get(cedente, 0)
            if restante_cedente > 0 and (restante_global is None or restante_global > 0):
                pendentes_liberados.append(registro)
                consumido_lote_por_cedente[cedente] = consumido_lote_por_cedente.get(cedente, 0) + 1
                if restante_total is not None:
                    consumido_lote_total += 1
            else:
                if id_registro is not None:
                    motivo = "Bloqueado por limite mensal (reserva no lote)"
                    if restante_global is not None and restante_global <= 0:
                        motivo = "Bloqueado por limite mensal total (reserva no lote)"
                    updates_bloqueio.append(
                        self._build_update(
                            id_registro=id_registro,
                            status="PENDENTE_BLOQUEADO",
                            status_desc=motivo,
                        )
                    )

        if updates_bloqueio:
            self._flush_updates(updates_bloqueio)
            self.logger.warning(
                "Pendentes bloqueados por limite mensal na reserva do lote: %s",
                len(updates_bloqueio)
            )

        return pendentes_liberados

    def _aplicar_reserva_limite_semanal(self, pendentes):
        if not pendentes:
            return pendentes

        resumo = EnvioRepository.buscar_resumo_semanal_limite_cedente()

        def _normalizar_cedente(valor):
            return str(valor or "").strip().upper()

        restante_por_cedente = {}
        for row in resumo:
            cedente = _normalizar_cedente(getattr(row, "CEDENTE", None))
            limite = int(
                getattr(
                    row,
                    "LIMITE_EFETIVO",
                    getattr(row, "LIMITE_SEMANAL", 0)
                ) or 0
            )
            total_enviado = int(getattr(row, "TOTAL_ENVIADO", 0) or 0)
            restante_por_cedente[cedente] = max(0, limite - total_enviado)

        consumido_lote_por_cedente = {}
        restante_total = restante_por_cedente.get(self.CEDENTE_LIMITE_TOTAL)
        consumido_lote_total = 0
        pendentes_liberados = []
        updates_bloqueio = []

        for registro in pendentes:
            cedente = _normalizar_cedente(getattr(registro, "CEDENTE", None))
            id_registro = getattr(registro, "ID", None)
            status_registro = str(getattr(registro, "STATUS", "") or "").upper()

            # APROVADO__GERENCIA e PENDENTE__APROVADO ignoram o limite semanal,
            # inclusive o limite total semanal.
            if status_registro in {"APROVADO__GERENCIA", "PENDENTE__APROVADO"}:
                pendentes_liberados.append(registro)
                continue

            if cedente not in restante_por_cedente:
                if id_registro is not None:
                    updates_bloqueio.append(
                        self._build_update(
                            id_registro=id_registro,
                            status="PENDENTE_BLOQUEADO",
                            status_desc="Bloqueado por cedente fora do padrao semanal",
                        )
                    )
                continue

            restante_global = None if restante_total is None else restante_total - consumido_lote_total
            restante_cedente = restante_por_cedente[cedente] - consumido_lote_por_cedente.get(cedente, 0)
            if restante_cedente > 0 and (restante_global is None or restante_global > 0):
                pendentes_liberados.append(registro)
                consumido_lote_por_cedente[cedente] = consumido_lote_por_cedente.get(cedente, 0) + 1
                if restante_total is not None:
                    consumido_lote_total += 1
            else:
                if id_registro is not None:
                    motivo = "Bloqueado por limite semanal (reserva no lote)"
                    if restante_global is not None and restante_global <= 0:
                        motivo = "Bloqueado por limite semanal total (reserva no lote)"
                    updates_bloqueio.append(
                        self._build_update(
                            id_registro=id_registro,
                            status="PENDENTE_BLOQUEADO",
                            status_desc=motivo,
                        )
                    )

        if updates_bloqueio:
            self._flush_updates(updates_bloqueio)
            self.logger.warning(
                "Pendentes bloqueados por limite semanal na reserva do lote: %s",
                len(updates_bloqueio)
            )

        return pendentes_liberados

    def _agrupar_bloqueios_por_cedente(self, rows):
        agrupado = {}
        for row in rows:
            id_cedente = getattr(row, "ID_CEDENTE", None)
            cedente = getattr(row, "CEDENTE", None)
            chave = (id_cedente, cedente)
            agrupado[chave] = agrupado.get(chave, 0) + 1

        resultado = []
        for (id_cedente, cedente), total in sorted(agrupado.items(), key=lambda x: (x[0][0] or 0, x[0][1] or "")):
            resultado.append({
                "id_cedente": id_cedente,
                "cedente": cedente,
                "total": total
            })
        return resultado

    def _bloquear_cedente_total(self):
        bloqueados = EnvioRepository.bloquear_pendentes_por_cedente_todos_status(
            cedente=self.CEDENTE_LIMITE_TOTAL,
            status_desc=self.STATUS_DESC_CEDENTE_RESERVADO
        )
        if bloqueados:
            self.logger.warning(
                "Pendentes bloqueados por cedente reservado TOTAL: %s",
                bloqueados
            )

    def enviar_relatorio_hora_manual(self):
        try:
            agora = datetime.now()
            ref_fim = agora.replace(minute=0, second=0, microsecond=0)
            ref_inicio = ref_fim - timedelta(hours=1)

            qtd = EnvioRepository.contar_enviados_periodo(ref_inicio, ref_fim)
            por_cedente = EnvioRepository.contar_enviados_por_cedente_periodo(ref_inicio, ref_fim)
            inicio_dia = ref_fim.replace(hour=0, minute=0, second=0, microsecond=0)
            total_dia = EnvioRepository.contar_enviados_periodo(inicio_dia, ref_fim)
            total_semana = EnvioRepository.contar_enviados_semana_total()
            total_mes = EnvioRepository.contar_enviados_mes_total()
            resumo_relatorio = EnvioRepository.buscar_resumo_relatorio_limite_cedente()

            Email.enviar_alerta_hora(
                qtd_enviadas=qtd,
                inicio=ref_inicio,
                fim=ref_fim,
                por_cedente=por_cedente,
                total_dia=total_dia,
                total_semana=total_semana,
                total_mes=total_mes,
                resumo_semanal=resumo_relatorio,
                permitir_qtd_zero=True
            )
            self.logger.info(
                "Relatorio manual enviado: %s - %s, total=%s",
                ref_inicio.strftime("%d/%m/%Y %H:%M"),
                ref_fim.strftime("%d/%m/%Y %H:%M"),
                qtd
            )
        except Exception as e:
            self.logger.exception("Falha ao enviar relatorio manual: %s", str(e))

    def _enviar_alerta_hora(self):
        try:
            agora = datetime.now()
            ref_fim = agora.replace(minute=0, second=0, microsecond=0)
            ref_inicio = ref_fim - timedelta(hours=1)
            # Envia apenas na virada da hora (janela curta)
            if agora < ref_fim or agora >= ref_fim + timedelta(minutes=5):
                return
            chave_alerta = ref_inicio.strftime("%Y-%m-%d %H:00")
            if chave_alerta in self._alerta_hora_enviado:
                return
            if EnvioRepository.existe_alerta_hora(ref_inicio):
                return

            qtd = EnvioRepository.contar_enviados_periodo(ref_inicio, ref_fim)
            por_cedente = EnvioRepository.contar_enviados_por_cedente_periodo(ref_inicio, ref_fim)
            inicio_dia = ref_fim.replace(hour=0, minute=0, second=0, microsecond=0)
            total_dia = EnvioRepository.contar_enviados_periodo(inicio_dia, ref_fim)
            total_semana = EnvioRepository.contar_enviados_semana_total()
            total_mes = EnvioRepository.contar_enviados_mes_total()
            resumo_relatorio = EnvioRepository.buscar_resumo_relatorio_limite_cedente()
            Email.enviar_alerta_hora(
                qtd_enviadas=qtd,
                inicio=ref_inicio,
                fim=ref_fim,
                por_cedente=por_cedente,
                total_dia=total_dia,
                total_semana=total_semana,
                total_mes=total_mes,
                resumo_semanal=resumo_relatorio
            )
            self.logger.info(
                "Alerta hora enviado: %s - %s, total=%s",
                ref_inicio.strftime("%d/%m/%Y %H:%M"),
                ref_fim.strftime("%d/%m/%Y %H:%M"),
                qtd
            )
            self._alerta_hora_enviado.add(chave_alerta)
            EnvioRepository.registrar_alerta_hora(ref_inicio, ref_fim, qtd)
        except Exception as e:
            self.logger.exception("Falha ao enviar alerta por hora: %s", str(e))

    def _aplicar_bloqueios_iniciais(self):
        self._bloquear_cedente_total()

        if self.limite_cedente_semana_ativo:
            self.logger.info("Iniciando bloqueios semanais")
            bloqueados_fora_padrao = EnvioRepository.bloquear_pendentes_fora_padrao_semanal(
                status_desc="Bloqueado por cedente fora do padrao",
                incluir_aprovado=self.bloquear_fora_padrao_semana_incluir_aprovado,
                logger=self.logger
            )
            if bloqueados_fora_padrao:
                self.logger.warning(
                    "Pendentes bloqueados por fora do padrao semanal: %s",
                    len(bloqueados_fora_padrao)
                )
                Email.enviar_alerta_cedente_fora_padrao(
                    data_ref=datetime.now(),
                    por_cedente=self._agrupar_bloqueios_por_cedente(bloqueados_fora_padrao)
                )

            self.logger.info("Iniciando bloqueio por limite semanal")
            bloqueados_semana = EnvioRepository.bloquear_pendentes_excedentes_semana(
                status_desc="Bloqueado por limite semanal",
                incluir_aprovado=self.limite_cedente_semana_incluir_aprovado,
                logger=self.logger
            )
            if bloqueados_semana:
                self.logger.warning(
                    "Pendentes bloqueados por limite semanal: %s",
                    len(bloqueados_semana)
                )
                Email.enviar_alerta_limite_semanal(
                    data_ref=datetime.now(),
                    por_cedente=self._agrupar_bloqueios_por_cedente(bloqueados_semana)
                )

        if self.limite_cedente_mes_ativo:
            self.logger.info("Iniciando bloqueios mensais")
            bloqueados_fora_padrao_mes = EnvioRepository.bloquear_pendentes_fora_padrao_mensal(
                status_desc="Bloqueado por cedente fora do padrao mensal",
                incluir_aprovado=self.bloquear_fora_padrao_mes_incluir_aprovado,
                logger=self.logger
            )
            if bloqueados_fora_padrao_mes:
                self.logger.warning(
                    "Pendentes bloqueados por fora do padrao mensal: %s",
                    len(bloqueados_fora_padrao_mes)
                )
                Email.enviar_alerta_cedente_fora_padrao(
                    data_ref=datetime.now(),
                    por_cedente=self._agrupar_bloqueios_por_cedente(bloqueados_fora_padrao_mes)
                )

            self.logger.info("Iniciando bloqueio por limite mensal")
            bloqueados_mes = EnvioRepository.bloquear_pendentes_excedentes_mes(
                status_desc="Bloqueado por limite mensal",
                incluir_aprovado=self.limite_cedente_mes_incluir_aprovado,
                logger=self.logger
            )
            if bloqueados_mes:
                self.logger.warning(
                    "Pendentes bloqueados por limite mensal: %s",
                    len(bloqueados_mes)
                )
                Email.enviar_alerta_limite_mensal(
                    data_ref=datetime.now(),
                    por_cedente=self._agrupar_bloqueios_por_cedente(bloqueados_mes)
                )

        cedentes_bloqueio_fila = EnvioRepository.buscar_cedentes_com_pendentes_acima(
            limite=self.limite_pendentes_cedente,
            ids_cedente_limite_especial=self.IDS_CEDENTE_LIMITE_ESPECIAL,
            limite_especial=self.limite_pendentes_cedente_especial,
            logger=self.logger
        )
        if cedentes_bloqueio_fila:
            for row in cedentes_bloqueio_fila:
                cedente = row.CEDENTE
                total = row.TOTAL
                self.logger.warning(
                    "Bloqueando pendentes do cedente %s (total_pendentes=%s)",
                    cedente,
                    total
                )
                EnvioRepository.bloquear_pendentes_por_cedente(
                    cedente=cedente,
                    status_desc=f"Bloqueado por volume pendente: {total}"
                )
                Email.enviar_alerta_pendentes_cedente(
                    cedente=cedente,
                    qtd_pendentes=total,
                    data_ref=datetime.now()
                )

        cedentes_bloqueio = EnvioRepository.buscar_cedentes_com_envios_ultima_hora_acima(
            limite=self.limite_cedente_hora,
            ids_cedente_limite_especial=self.IDS_CEDENTE_LIMITE_ESPECIAL,
            limite_especial=self.limite_cedente_hora_especial,
            logger=self.logger
        )
        if cedentes_bloqueio:
            for row in cedentes_bloqueio:
                cedente = row.CEDENTE
                total = row.TOTAL
                self.logger.warning(
                    "Cedente acima do limite/hora, sem envio neste ciclo (%s, total_ultima_hora=%s)",
                    cedente,
                    total
                )

    def _obter_limite_global_hora(self, canal=None):
        possui_cedente_especial = any(
            EnvioRepository.existe_pendente_elegivel_por_cedente(id_cedente, canal=canal)
            for id_cedente in self.IDS_CEDENTE_LIMITE_ESPECIAL
        )
        if possui_cedente_especial:
            return self.limite_global_hora_especial
        return self.limite_global_hora

    def _preparar_registro(self, registro):
        id_registro = registro.ID
        custom_message_id = registro.CUSTOM_MESSAGE_ID
        cedente = registro.CEDENTE
        nm_usuario_envio = registro.NM_USUARIO_ENVIO
        numero_original = registro.NUM_CLIENTE
        to = self._normalizar_numero(registro.NUM_CLIENTE)
        mensagem = registro.MENSAGEM
        mensagem_banco_original = "" if mensagem is None else str(mensagem)
        mensagem_template = getattr(registro, "MENSAGEM_TEMPLATE", None)
        variaveis_json = getattr(registro, "VARIAVEIS_JSON", None)
        canal = getattr(registro, "CANAL", None)
        status_registro = getattr(registro, "STATUS", None)

        try:
            if (
                self.filtro_canal_ativo
                and str(status_registro or "").upper() not in self.STATUSS_LIBERADOS
                and str(canal or "").upper() != str(self.canal_envio_permitido or "").upper()
            ):
                return None, None

            if not custom_message_id or str(custom_message_id).strip() == "":
                custom_message_id = build_custom_message_id(
                    cedente=cedente,
                    nm_usuario_envio=nm_usuario_envio,
                    id_registro=id_registro
                )
            else:
                custom_message_id = ensure_unique_custom_message_id(
                    custom_message_id,
                    id_registro=id_registro
                )

            motivo_numero = self._validar_numero_basico(numero_original)
            if motivo_numero:
                self.logger.warning(
                    "Mensagem bloqueada por numero invalido (ID=%s, motivo=%s)",
                    id_registro,
                    motivo_numero
                )
                update = self._build_update(
                    id_registro=id_registro,
                    status="MSG_FORA_PADRAO",
                    status_desc=motivo_numero,
                    custom_message_id=custom_message_id,
                    mensagem=mensagem
                )
                return None, update

            if mensagem_template:
                try:
                    mensagem = render_template(
                        mensagem_template,
                        variaveis_json
                    )
                except ValueError as exc:
                    motivo = f"ERRO VARIAVEIS: {str(exc)}"
                    self.logger.warning(
                        "Mensagem bloqueada por variaveis (ID=%s, erro=%s)",
                        id_registro,
                        str(exc)
                    )
                    update = self._build_update(
                        id_registro=id_registro,
                        status="MSG_FORA_PADRAO",
                        status_desc=motivo,
                        custom_message_id=custom_message_id,
                        mensagem=mensagem
                    )
                    return None, update

            mensagem_original = "" if mensagem is None else str(mensagem)
            try:
                rcs_content = self._montar_conteudo_card(
                    mensagem_original,
                    variaveis_json
                )
            except ValueError as exc:
                motivo = f"JSON CARD INVALIDO: {str(exc)}"
                self.logger.warning(
                    "Mensagem bloqueada por card invalido (ID=%s, erro=%s)",
                    id_registro,
                    str(exc)
                )
                update = self._build_update(
                    id_registro=id_registro,
                    status="MSG_FORA_PADRAO",
                    status_desc=motivo,
                    custom_message_id=custom_message_id,
                    mensagem=mensagem_original
                )
                return None, update

            mensagem_final = mensagem_original

            pre_update = None
            if (
                mensagem_final != mensagem_banco_original
                or not getattr(registro, "CUSTOM_MESSAGE_ID", None)
            ):
                pre_update = self._build_update(
                    id_registro=id_registro,
                    status=None,
                    custom_message_id=custom_message_id,
                    mensagem=mensagem_final
                )

            preparado = {
                "id_registro": id_registro,
                "cedente": cedente,
                "to": to,
                "content": rcs_content,
                "custom_message_id": custom_message_id,
                "mensagem": mensagem_final
            }
            return preparado, pre_update
        except Exception as e:
            update = self._build_update(
                id_registro=id_registro,
                status="ERRO",
                status_desc=str(e),
                custom_message_id=custom_message_id,
                mensagem=mensagem
            )
            return None, update

    def _obter_limite_caracteres(self, cedente, canal):
        return 3072

    def _montar_conteudo_card(self, mensagem, variaveis_json):
        if mensagem is None or str(mensagem).strip() == "":
            raise ValueError("MENSAGEM e obrigatoria para a descricao do card")
        if len(str(mensagem)) > 2000:
            raise ValueError("MENSAGEM excede 2000 caracteres")

        if not variaveis_json or str(variaveis_json).strip() == "":
            raise ValueError("VARIAVEIS_JSON e obrigatorio para montar o card")

        try:
            config = json.loads(variaveis_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(f"VARIAVEIS_JSON deve ser JSON valido ({str(exc)})")

        if not isinstance(config, dict):
            raise ValueError("VARIAVEIS_JSON deve ser um objeto JSON")

        config = deepcopy(config)
        titulo = config.get("titulo") or config.get("title")
        if titulo is not None and len(str(titulo)) > 200:
            raise ValueError("titulo/title excede 200 caracteres")

        imagem_url = (
            config.get("imagem")
            or config.get("imagemUrl")
            or config.get("imageUrl")
            or config.get("mediaUrl")
        )
        self._validar_url(imagem_url, "imagem/imageUrl", 1000)

        orientation = str(config.get("orientation") or config.get("orientacao") or "VERTICAL").upper()
        alignment = str(config.get("alignment") or config.get("alinhamento") or "LEFT").upper()
        height = str(config.get("height") or config.get("altura") or "MEDIUM").upper()

        if orientation not in ("HORIZONTAL", "VERTICAL"):
            raise ValueError("orientation deve ser HORIZONTAL ou VERTICAL")
        if alignment not in ("LEFT", "RIGHT"):
            raise ValueError("alignment deve ser LEFT ou RIGHT")
        if height not in ("SHORT", "MEDIUM", "TALL"):
            raise ValueError("height/altura deve ser SHORT, MEDIUM ou TALL")

        botoes = config.get("botoes") or config.get("buttons") or config.get("suggestions")
        if not isinstance(botoes, list) or not botoes:
            raise ValueError("botoes/buttons e obrigatorio em VARIAVEIS_JSON")
        if len(botoes) > 4:
            raise ValueError("botoes/buttons aceita no maximo 4 botoes no card")

        suggestions = []
        for index, botao in enumerate(botoes, start=1):
            suggestions.append(self._normalizar_botao_card(botao, index))

        card_content = {
            "description": str(mensagem),
            "media": {
                "file": {
                    "url": str(imagem_url).strip()
                },
                "height": height
            },
            "suggestions": suggestions
        }
        if titulo is not None and str(titulo).strip() != "":
            card_content["title"] = str(titulo)

        return {
            "type": "CARD",
            "orientation": orientation,
            "alignment": alignment,
            "content": card_content
        }

    def _normalizar_botao_card(self, botao, index):
        if not isinstance(botao, dict):
            raise ValueError(f"botao {index} deve ser objeto JSON")

        tipo = str(botao.get("tipo") or botao.get("type") or "OPEN_URL").upper()
        if tipo in ("BROWSER", "URL", "URL_ABERTO", "URL_ABERTA"):
            tipo = "OPEN_URL"
        if tipo != "OPEN_URL":
            raise ValueError(f"botao {index} deve ser URL/OPEN_URL")

        text = botao.get("texto") or botao.get("text")
        postback = botao.get("retorno") or botao.get("postback") or botao.get("postbackData") or text
        url = botao.get("url") or botao.get("website") or botao.get("websiteUrl")
        suggestion = {
            "type": "OPEN_URL",
            "text": text,
            "postbackData": postback
        }

        self._validar_texto_obrigatorio(suggestion, "text", 25, prefix=f"botao {index}")
        self._validar_texto_obrigatorio(suggestion, "postbackData", 2048, prefix=f"botao {index}")
        self._validar_url(url, f"botao {index}.url", 2048)
        suggestion["url"] = str(url).strip()

        return suggestion

    def _validar_suggestion(self, suggestion, index):
        if not isinstance(suggestion, dict):
            raise ValueError(f"suggestion {index} deve ser objeto JSON")

        suggestion_type = str(suggestion.get("type") or "").upper()
        if suggestion_type not in ("OPEN_URL", "REPLY", "DIAL_PHONE"):
            raise ValueError(f"suggestion {index} type invalido")
        suggestion["type"] = suggestion_type

        self._validar_texto_obrigatorio(suggestion, "text", 25, prefix=f"suggestion {index}")
        self._validar_texto_obrigatorio(suggestion, "postbackData", 2048, prefix=f"suggestion {index}")

        if suggestion_type == "OPEN_URL":
            self._validar_url(suggestion.get("url"), f"suggestion {index}.url", 2048)
        elif suggestion_type == "DIAL_PHONE":
            self._validar_texto_obrigatorio(suggestion, "phoneNumber", 20, prefix=f"suggestion {index}")

    def _validar_texto_obrigatorio(self, data, field, max_len, prefix=None):
        label = f"{prefix}.{field}" if prefix else field
        value = data.get(field)
        if value is None or str(value).strip() == "":
            raise ValueError(f"{label} e obrigatorio")
        if len(str(value)) > max_len:
            raise ValueError(f"{label} excede {max_len} caracteres")
        data[field] = str(value)

    def _validar_url(self, value, field, max_len):
        if value is None or str(value).strip() == "":
            raise ValueError(f"{field} e obrigatorio")
        value = str(value).strip()
        if len(value) > max_len:
            raise ValueError(f"{field} excede {max_len} caracteres")
        if not value.startswith(("http://", "https://")):
            raise ValueError(f"{field} deve comecar com http:// ou https://")

    def _enviar_batch(self, preparados):
        if not preparados:
            return []

        if self.modo_teste_sem_envio:
            updates = []
            for item in preparados:
                infobip_id = f"TEST-{item['id_registro']}"
                updates.append(
                    self._build_update(
                        id_registro=item["id_registro"],
                        status="ENVIADO",
                        status_desc="pending",
                        id_mensagem=infobip_id,
                        response_json=json.dumps({
                            "modo_teste_sem_envio": True,
                            "to": item["to"],
                            "custom_message_id": item["custom_message_id"],
                            "content_type": item["content"].get("type"),
                        }),
                        bulk_id=None,
                        custom_message_id=item["custom_message_id"],
                        mensagem=item.get("mensagem")
                    )
                )
            return updates

        response = self.client.send_rcs_batch(
            messages=[
                {
                    "to": item["to"],
                    "content": item["content"],
                    "custom_message_id": item["custom_message_id"]
                }
                for item in preparados
            ]
        )

        updates = []
        if response["http_status"] >= 200 and response["http_status"] < 300:
            response_json = response["response_json"] or {}
            bulk_id = response_json.get("bulkId") or response_json.get("bulk_id")
            messages = response_json.get("messages") or []

            if len(messages) == len(preparados):
                for item, msg in zip(preparados, messages):
                    infobip_id = msg.get("messageId")
                    if infobip_id:
                        updates.append(
                            self._build_update(
                                id_registro=item["id_registro"],
                                status="ENVIADO",
                                status_desc="pending",
                                id_mensagem=infobip_id,
                                response_json=json.dumps(response_json),
                                bulk_id=bulk_id,
                                custom_message_id=item["custom_message_id"],
                                mensagem=item.get("mensagem")
                            )
                        )
                    else:
                        updates.append(
                            self._build_update(
                                id_registro=item["id_registro"],
                                status="ERRO",
                                status_desc="Resposta sem messageId para o envio",
                                response_json=json.dumps(response_json),
                                bulk_id=bulk_id,
                                custom_message_id=item["custom_message_id"],
                                mensagem=item.get("mensagem")
                            )
                        )
            else:
                for item in preparados:
                    updates.append(
                        self._build_update(
                            id_registro=item["id_registro"],
                            status="ERRO",
                            status_desc="Resposta sem correlacao por lote",
                            response_json=json.dumps(response_json),
                            bulk_id=bulk_id,
                            custom_message_id=item["custom_message_id"],
                            mensagem=item.get("mensagem")
                        )
                    )
        else:
            for item in preparados:
                updates.append(
                    self._build_update(
                        id_registro=item["id_registro"],
                        status="ERRO",
                        status_desc=response["raw_response"],
                        response_json=response["raw_response"],
                        custom_message_id=item["custom_message_id"],
                        mensagem=item.get("mensagem")
                    )
                )

        if updates:
            backup_file = self._persistir_retorno_infobip(preparados, response, updates)
            for upd in updates:
                upd["backup_file"] = backup_file

        return updates

    def _build_update(
        self,
        *,
        id_registro,
        status,
        status_desc=None,
        id_mensagem=None,
        response_json=None,
        bulk_id=None,
        custom_message_id=None,
        mensagem=None
    ):
        return {
            "id_registro": id_registro,
            "status": status,
            "status_desc": status_desc,
            "id_mensagem": id_mensagem,
            "response_json": response_json,
            "bulk_id": bulk_id,
            "custom_message_id": custom_message_id,
            "mensagem": mensagem,
        }

    def _flush_updates(self, updates):
        try:
            EnvioRepository.atualizar_envio_batch(
                updates,
                batch_size=self.update_batch_tamanho,
                logger=self.logger
            )
        except Exception:
            self._garantir_backup_persistido(updates)
            self.logger.exception(
                "Falha ao gravar updates de envio no banco; backups mantidos em spool para reconciliacao"
            )
            raise
        else:
            self._limpar_backups_processados(updates)

    def _reconciliar_spool_envio(self):
        arquivos = sorted(self.spool_dir.glob("*.json"))
        if not arquivos:
            return

        self.logger.warning("Reconhecendo spool de envio pendente: arquivos=%s", len(arquivos))

        for path in arquivos:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                self.logger.exception("Falha ao ler arquivo de spool de envio: %s", path)
                continue

            updates = payload.get("updates") or []
            if not updates:
                self.logger.warning("Arquivo de spool sem updates, removendo: %s", path)
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    self.logger.exception("Falha ao remover arquivo de spool vazio: %s", path)
                continue

            for upd in updates:
                upd["backup_file"] = str(path)

            try:
                self.logger.warning(
                    "Reaplicando spool de envio no banco: arquivo=%s updates=%s",
                    path,
                    len(updates)
                )
                self._flush_updates(updates)
            except Exception:
                self.logger.exception("Falha ao reconciliar spool de envio: %s", path)
                break

    def _persistir_reserva_execucao(self, ids_reserva):
        if not ids_reserva:
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        path = self.reserva_spool_dir / f"reserva_{timestamp}.json"
        payload = {
            "created_at": datetime.now().isoformat(),
            "ids_reserva": ids_reserva,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.logger.warning(
            "Reserva de execucao persistida em spool: arquivo=%s itens=%s",
            path,
            len(ids_reserva)
        )
        return path

    def _reconciliar_reservas_pendentes(self):
        arquivos = sorted(self.reserva_spool_dir.glob("*.json"))
        if not arquivos:
            return

        self.logger.warning("Reconhecendo reservas pendentes: arquivos=%s", len(arquivos))

        for path in arquivos:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                ids_reserva = [int(x) for x in (payload.get("ids_reserva") or []) if x is not None]
            except Exception:
                self.logger.exception("Falha ao ler arquivo de reserva pendente: %s", path)
                continue

            if not ids_reserva:
                self.logger.warning("Arquivo de reserva sem IDs, removendo: %s", path)
                self._remover_reserva_execucao(path)
                continue

            try:
                rows = EnvioRepository.marcar_nao_enviado_reservados_batch(
                    ids_reserva,
                    status_desc="Nao enviado: processo interrompido antes da confirmacao de envio",
                    batch_size=self.update_batch_tamanho,
                    logger=self.logger
                )
                self.logger.warning(
                    "Reserva pendente reconciliada: arquivo=%s itens=%s rows_atualizadas=%s",
                    path,
                    len(ids_reserva),
                    rows
                )
            except Exception:
                self.logger.exception("Falha ao reconciliar reserva pendente: %s", path)
                break
            else:
                self._remover_reserva_execucao(path)

    def _persistir_retorno_infobip(self, preparados, response, updates):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        response_json = response.get("response_json") or {}
        bulk_id = None
        if isinstance(response_json, dict):
            bulk_id = response_json.get("bulkId") or response_json.get("bulk_id")

        nome = f"envio_{timestamp}"
        if bulk_id:
            nome += f"_{bulk_id}"
        path = self.spool_dir / f"{nome}.json"

        payload = {
            "created_at": datetime.now().isoformat(),
            "http_status": response.get("http_status"),
            "bulk_id": bulk_id,
            "prepared": [
                {
                    "id_registro": item.get("id_registro"),
                    "to": item.get("to"),
                    "custom_message_id": item.get("custom_message_id"),
                    "mensagem": item.get("mensagem"),
                }
                for item in preparados
            ],
            "response_json": response.get("response_json"),
            "raw_response": response.get("raw_response"),
            "updates": [
                {
                    "id_registro": upd.get("id_registro"),
                    "status": upd.get("status"),
                    "status_desc": upd.get("status_desc"),
                    "id_mensagem": upd.get("id_mensagem"),
                    "bulk_id": upd.get("bulk_id"),
                    "custom_message_id": upd.get("custom_message_id"),
                    "mensagem": upd.get("mensagem"),
                }
                for upd in updates
            ],
        }

        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.logger.warning("Retorno Infobip persistido em spool: %s", path)
        return str(path)

    def _garantir_backup_persistido(self, updates):
        for upd in updates:
            backup_file = upd.get("backup_file")
            if backup_file and os.path.exists(backup_file):
                continue

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            path = self.spool_dir / f"envio_flush_failure_{timestamp}_{upd.get('id_registro')}.json"
            payload = {
                "created_at": datetime.now().isoformat(),
                "reason": "flush_failure_without_prior_spool",
                "updates": [
                    {
                        "id_registro": upd.get("id_registro"),
                        "status": upd.get("status"),
                        "status_desc": upd.get("status_desc"),
                        "id_mensagem": upd.get("id_mensagem"),
                        "bulk_id": upd.get("bulk_id"),
                        "custom_message_id": upd.get("custom_message_id"),
                        "mensagem": upd.get("mensagem"),
                    }
                ],
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            upd["backup_file"] = str(path)
            self.logger.warning("Backup emergencial de envio salvo em spool: %s", path)

    def _limpar_backups_processados(self, updates):
        paths = {
            upd.get("backup_file")
            for upd in updates
            if upd.get("backup_file")
        }
        for path in paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    self.logger.info("Backup de spool removido apos persistencia no banco: %s", path)
            except Exception:
                self.logger.exception("Falha ao remover backup de spool processado: %s", path)

    def _remover_reserva_execucao(self, path):
        if not path:
            return
        try:
            if os.path.exists(path):
                os.remove(path)
                self.logger.info("Reserva de execucao removida do spool: %s", path)
        except Exception:
            self.logger.exception("Falha ao remover arquivo de reserva pendente: %s", path)

    def _normalizar_numero(self, numero):
        if numero is None:
            return None
        bruto = str(numero).strip()
        if bruto == "":
            return bruto
        digitos = "".join(ch for ch in bruto if ch.isdigit())
        if digitos == "":
            return digitos
        # Considera "55" como DDI apenas quando o tamanho ja corresponde ao formato BR com DDI:
        # 55 + DDD + numero (12 ou 13 digitos).
        if digitos.startswith("55") and len(digitos) in (12, 13):
            return digitos
        return f"55{digitos}"

    def _validar_numero_basico(self, numero):
        if numero is None:
            return "NUMERO INVALIDO: VAZIO OU EM BRANCO"

        bruto = str(numero).strip()
        if bruto == "":
            return "NUMERO INVALIDO: VAZIO OU EM BRANCO"

        digitos = "".join(ch for ch in bruto if ch.isdigit())
        if digitos == "":
            return "NUMERO INVALIDO: VAZIO OU EM BRANCO"

        numero_sem_ddi = digitos[2:] if digitos.startswith("55") and len(digitos) > 2 else digitos
        if numero_sem_ddi and len(set(numero_sem_ddi)) == 1:
            return "NUMERO INVALIDO: TODOS OS DIGITOS IGUAIS"

        return None

