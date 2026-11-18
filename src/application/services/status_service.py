import requests
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from datetime import datetime, timedelta
from src.infrastructure.infobip.client import InfobipClient
from src.infrastructure.logging.logger import get_logger
from src.repository.envio_repository import EnvioRepository


def _parse_infobip_datetime(value):
    if not value:
        return None
    value = str(value).strip()
    if len(value) >= 5 and value[-5] in ("+", "-") and value[-3] != ":":
        value = value[:-2] + ":" + value[-2:]
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _to_sql_datetime(value):
    if not value:
        return None
    # SQL style 121: yyyy-mm-dd hh:mi:ss.mmm
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _clip(value, max_len: int):
    if value is None:
        return None
    text = str(value)
    if len(text) <= max_len:
        return text
    return text[:max_len]


def _bulk_attempts_from_status_desc(value):
    if not value:
        return 0
    text = str(value)
    marker = "tentativa bulk "
    start = text.find(marker)
    if start < 0:
        return 0
    start += len(marker)
    end = text.find("/2", start)
    if end < 0:
        return 0
    try:
        return int(text[start:end])
    except ValueError:
        return 0


class StatusService:
    def __init__(self):
        self.logger = get_logger("status")
        self.client = InfobipClient(logger=self.logger)
        self.max_workers = 4
        # Infobip logs/reports limit is up to 1000 per request.
        self.status_batch_tamanho = 1000
        self.status_update_batch_tamanho = 150
        self.status_pendentes_limit = 30000
        # Inicia a primeira consulta de status apenas apos 2 horas do envio.
        self.status_primeira_consulta_delay_minutes = 120
        # Limita o volume do fluxo 1-por-1 por rodada para respeitar o rate limit.
        self.status_fallback_limit = 600
        # Depois da primeira consulta, espera 1 hora para tentar novamente.
        self.status_min_interval_minutes = 60
        self.status_bulk_max_attempts = 2
        self.status_retry_unique_after_hours = 24
        self.status_bulk_ids_limit = 200
        self.status_logs_batches = 0
        self.status_use_bulk = True
        self.status_single_max_api_calls_per_item = 2
        self.status_api_rate_per_second = 2
        self.status_chunk_timeout_margin_seconds = 30
        self.arquivar_apos_status = True

    def processar_status(self):
        self.logger.info("Iniciando processamento de status")
        self.logger.info("Expirando pendentes antigos")
        EnvioRepository.expirar_pendentes_antigos(logger=self.logger)
        self.logger.info("Marcando como SEM_RETORNO os casos com mais de 2 dias")
        EnvioRepository.marcar_sem_retorno_antigos(logger=self.logger)
        self.logger.info("Buscando pendentes para atualizacao de status")
        pendentes = EnvioRepository.buscar_enviados_pendentes_status(
            limit=self.status_pendentes_limit,
            min_sent_age_minutes=self.status_primeira_consulta_delay_minutes,
            min_interval_minutes=self.status_min_interval_minutes,
            logger=self.logger
        )

        if not pendentes:
            self.logger.info("Pendentes status: 0")
            self._arquivar_envios_antigos()
            return

        self.logger.info("Pendentes status: %s", len(pendentes))

        by_message = {}
        rows_by_bulk = {}
        for row in pendentes:
            mid = getattr(row, "ID_MENSAGEM", None)
            if mid:
                by_message[str(mid)] = row
            bulk_id = getattr(row, "BULK_ID", None)
            if bulk_id:
                rows_by_bulk.setdefault(str(bulk_id), []).append(row)

        bulk_ids = self._selecionar_bulk_ids_cache(rows_by_bulk)

        bulk_ids_set = {str(bulk_id) for bulk_id in bulk_ids if bulk_id}
        restantes = self._selecionar_pendentes_individuais(pendentes, bulk_ids_set)
        self.logger.info("Pendentes status 1-por-1: %s", len(restantes))

        futures = {}
        resultados = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            if self.status_use_bulk and bulk_ids:
                futures[executor.submit(
                    self._processar_fluxo_bulk,
                    bulk_ids,
                    by_message,
                    rows_by_bulk
                )] = "bulk"
            if restantes:
                futures[executor.submit(self._processar_fluxo_individual, restantes)] = "individual"

            for future, nome_fluxo in futures.items():
                try:
                    resultados.append(future.result())
                except Exception:
                    self.logger.exception("Falha no fluxo de status: %s", nome_fluxo)
                    raise

        total_updates = sum(item.get("updates", 0) for item in resultados)
        self.logger.info("Total atualizado: %s", total_updates)
        self._arquivar_envios_antigos()

    def _arquivar_envios_antigos(self):
        if not self.arquivar_apos_status:
            return

        try:
            self.logger.info("Iniciando arquivamento RCS")
            EnvioRepository.arquivar_envios_antigos(logger=self.logger)
        except Exception:
            self.logger.exception("Falha ao arquivar envios RCS antigos")

    def _selecionar_bulk_ids_cache(self, rows_by_bulk):
        if not self.status_use_bulk or not rows_by_bulk:
            return []

        candidatos = []
        for bulk_id, rows in rows_by_bulk.items():
            pendentes_bulk = [
                row for row in rows
                if _bulk_attempts_from_status_desc(getattr(row, "STATUS_DESC", None)) < self.status_bulk_max_attempts
            ]
            if not pendentes_bulk:
                continue
            candidatos.append((bulk_id, len(pendentes_bulk)))

        candidatos.sort(key=lambda item: item[1], reverse=True)
        if self.status_bulk_ids_limit > 0:
            candidatos = candidatos[: self.status_bulk_ids_limit]

        bulk_ids = [bulk_id for bulk_id, _ in candidatos]
        self.logger.info("BulkIds pendentes de status (cache): %s", len(bulk_ids))
        return bulk_ids

    def _selecionar_pendentes_individuais(self, pendentes, bulk_ids_em_andamento):
        limite_retry_unico = datetime.now() - timedelta(hours=self.status_retry_unique_after_hours)
        selecionados = []

        for row in pendentes:
            bulk_id = getattr(row, "BULK_ID", None)
            dt_envio = getattr(row, "DT_ENVIO", None)
            if not bulk_id:
                selecionados.append(row)
                continue

            if dt_envio is not None and dt_envio <= limite_retry_unico:
                selecionados.append(row)
                continue

            if str(bulk_id) in bulk_ids_em_andamento:
                continue

            if (
                _bulk_attempts_from_status_desc(getattr(row, "STATUS_DESC", None)) >= self.status_bulk_max_attempts
                and dt_envio is not None
                and dt_envio <= limite_retry_unico
            ):
                selecionados.append(row)

        selecionados.sort(
            key=lambda row: (
                getattr(row, "DT_ENVIO", None) is None,
                getattr(row, "DT_ENVIO", None),
                getattr(row, "ID", None) or 0,
            )
        )

        if self.status_fallback_limit > 0 and len(selecionados) > self.status_fallback_limit:
            self.logger.info(
                "Fallback status limitado (faltantes=%s, limite=%s)",
                len(selecionados),
                self.status_fallback_limit
            )
            return selecionados[: self.status_fallback_limit]

        return selecionados

    def _processar_fluxo_bulk(self, bulk_ids, by_message, rows_by_bulk):
        total_updates = 0
        total_marcados = 0

        for bulk_id in bulk_ids:
            self.logger.info(
                "Consultando bulkId=%s registros_locais=%s",
                bulk_id,
                len(rows_by_bulk.get(str(bulk_id), []))
            )
            updates = self._processar_bulk_id(bulk_id, by_message, set())
            if updates:
                self._flush_status_updates(updates)
                total_updates += len(updates)

            ids_finalizados = {
                upd.get("id_registro")
                for upd in (updates or [])
                if upd.get("id_registro") and upd.get("final_status")
            }
            ids_bulk_pendentes = [
                getattr(row, "ID", None)
                for row in rows_by_bulk.get(str(bulk_id), [])
                if getattr(row, "ID", None) is not None
                and getattr(row, "ID", None) not in ids_finalizados
            ]
            if ids_bulk_pendentes:
                EnvioRepository.marcar_tentativa_bulk_batch(ids_bulk_pendentes)
                total_marcados += len(ids_bulk_pendentes)
            self.logger.info(
                "Resultado bulkId=%s updates=%s pendentes_marcados=%s",
                bulk_id,
                len(updates or []),
                len(ids_bulk_pendentes)
            )

        self.logger.info(
            "Fluxo bulk finalizado: bulk_ids=%s updates=%s marcados=%s",
            len(bulk_ids),
            total_updates,
            total_marcados
        )
        return {
            "updates": total_updates,
            "bulk_ids": len(bulk_ids),
            "marcados": total_marcados,
        }

    def _processar_fluxo_individual(self, registros):
        total_updates = 0
        total_sem_update = 0

        for start in range(0, len(registros), self.status_update_batch_tamanho):
            chunk = registros[start:start + self.status_update_batch_tamanho]
            updates = []
            chunk_timeout = self._calcular_timeout_fluxo_individual(len(chunk))
            self.logger.info(
                "Iniciando lote status 1-por-1: inicio=%s fim=%s itens=%s timeout=%ss workers=%s",
                start,
                start + len(chunk) - 1,
                len(chunk),
                chunk_timeout,
                self.max_workers
            )

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._processar_registro, registro)
                    for registro in chunk
                ]
                try:
                    for future in as_completed(futures, timeout=chunk_timeout):
                        try:
                            update = future.result(timeout=15)
                            if update:
                                updates.append(update)
                        except TimeoutError:
                            self.logger.warning("Timeout future status (1-por-1)")
                        except Exception as e:
                            self.logger.exception("Excecao thread status (1-por-1): %s", str(e))
                except TimeoutError:
                    self.logger.warning(
                        "Timeout aguardando futures status (1-por-1) - cancelando pendentes (chunk=%s timeout=%ss)",
                        len(chunk),
                        chunk_timeout
                    )
                    for future in futures:
                        if not future.done():
                            future.cancel()

            if updates:
                self._flush_status_updates(updates)
                total_updates += len(updates)

            ids_sem_update = []
            updates_ids = {u.get("id_registro") for u in updates if u.get("id_registro")}
            for registro in chunk:
                id_registro = getattr(registro, "ID", None)
                if id_registro and id_registro not in updates_ids:
                    ids_sem_update.append(id_registro)
            if ids_sem_update:
                EnvioRepository.marcar_sem_retorno_status_batch(ids_sem_update)
                total_sem_update += len(ids_sem_update)

            self.logger.info(
                "Lote status 1-por-1 processado: itens=%s updates=%s sem_update=%s",
                len(chunk),
                len(updates),
                len(ids_sem_update)
            )

        self.logger.info(
            "Fluxo 1-por-1 finalizado: itens=%s updates=%s sem_update=%s",
            len(registros),
            total_updates,
            total_sem_update
        )
        return {
            "updates": total_updates,
            "sem_update": total_sem_update,
            "itens": len(registros),
        }

    def _calcular_timeout_fluxo_individual(self, chunk_size):
        if chunk_size <= 0:
            return 30

        chamadas_estimadas = chunk_size * self.status_single_max_api_calls_per_item
        segundos_estimados = chamadas_estimadas / max(1, self.status_api_rate_per_second)
        return int(segundos_estimados + self.status_chunk_timeout_margin_seconds)

    def _processar_registro(self, registro):
        id_registro = registro.ID
        id_mensagem = registro.ID_MENSAGEM
        cedente = getattr(registro, "CEDENTE", None)

        try:
            results = []
            self.logger.info(
                "Consultando status 1-por-1 ID=%s messageId=%s bulkId=%s dt_envio=%s",
                id_registro,
                id_mensagem,
                getattr(registro, "BULK_ID", None),
                getattr(registro, "DT_ENVIO", None)
            )

            response = self.client.get_rcs_reports(
                message_id=id_mensagem,
                limit=1
            )
            self.logger.info(
                "Resposta reports ID=%s http=%s",
                id_registro,
                response.get("http_status")
            )
            if response["http_status"] >= 200 and response["http_status"] < 300:
                data = response["response_json"] or {}
                results = data.get("results") or []
                if not results and isinstance(data, dict) and data.get("messageId"):
                    results = [data]
                self.logger.info(
                    "Reports ID=%s resultados=%s",
                    id_registro,
                    len(results)
                )
            else:
                self.logger.warning(
                    "Reports sem sucesso para ID=%s (http=%s), tentando logs",
                    id_registro,
                    response.get("http_status")
                )

            if not results:
                response = self.client.get_rcs_logs(
                    message_id=id_mensagem,
                    limit=1
                )
                self.logger.info(
                    "Resposta logs ID=%s http=%s",
                    id_registro,
                    response.get("http_status")
                )
                if response["http_status"] >= 200 and response["http_status"] < 300:
                    data = response["response_json"] or {}
                    results = data.get("results") or []
                    if not results and isinstance(data, dict) and data.get("messageId"):
                        results = [data]
                    self.logger.info(
                        "Logs ID=%s resultados=%s",
                        id_registro,
                        len(results)
                    )

            if not results:
                self.logger.info("Sem retorno Infobip para ID=%s messageId=%s", id_registro, id_mensagem)
                return

            report = results[0]
            update = self._build_status_update(
                report=report,
                id_registro=id_registro,
                cedente=cedente
            )
            self.logger.info(
                "Update montado ID=%s status_infobip=%s final_status=%s",
                id_registro,
                update.get("status"),
                update.get("final_status")
            )
            return update
        except requests.exceptions.RetryError as e:
            self.logger.warning(
                "Rate limit/RetryError ao consultar status ID=%s: %s",
                id_registro,
                str(e)
            )
            return
        except requests.exceptions.ConnectionError as e:
            self.logger.warning(
                "ConnectionError ao consultar status ID=%s: %s",
                id_registro,
                str(e)
            )
            return
        except Exception as e:
            EnvioRepository.atualizar_status(
                id_registro=id_registro,
                status_desc=str(e)
            )
            if cedente:
                self.logger.exception("Excecao status ID=%s CEDENTE=%s", id_registro, cedente)
            raise

    def _processar_logs_batch(self, by_message, skip_ids):
        response = self.client.get_rcs_logs_batch(limit=self.status_batch_tamanho)
        if not (200 <= response["http_status"] < 300):
            self.logger.warning("Erro ao consultar status em lote: %s", response["raw_response"])
            return []

        data = response["response_json"] or {}
        results = data.get("results") or []
        if not results and isinstance(data, dict) and data.get("messageId"):
            results = [data]
        if not results:
            return []

        updates = []
        for report in results:
            msg_id = report.get("messageId")
            if not msg_id:
                continue
            if str(msg_id) in skip_ids:
                continue
            row = by_message.get(str(msg_id))
            if not row:
                continue
            update = self._build_status_update(
                report=report,
                id_registro=row.ID,
                cedente=getattr(row, "CEDENTE", None)
            )
            if update:
                updates.append(update)
        return updates

    def _processar_bulk_id(self, bulk_id, by_message, skip_ids):
        response = self.client.get_rcs_reports_bulk(
            bulk_id=bulk_id,
            limit=self.status_batch_tamanho
        )
        self.logger.info(
            "Resposta reports bulkId=%s http=%s",
            bulk_id,
            response.get("http_status")
        )
        if not (200 <= response["http_status"] < 300):
            self.logger.warning(
                "Erro ao consultar status por bulkId=%s: %s",
                bulk_id,
                response["raw_response"]
            )
            return []

        data = response["response_json"] or {}
        results = data.get("results") or []
        if not results and isinstance(data, dict) and data.get("messageId"):
            results = [data]
        self.logger.info(
            "Reports bulkId=%s resultados=%s",
            bulk_id,
            len(results)
        )
        if not results:
            return []

        updates = []
        for report in results:
            msg_id = report.get("messageId")
            if not msg_id:
                continue
            if str(msg_id) in skip_ids:
                continue
            row = by_message.get(str(msg_id))
            if not row:
                continue
            update = self._build_status_update(
                report=report,
                id_registro=row.ID,
                cedente=getattr(row, "CEDENTE", None)
            )
            if update:
                updates.append(update)
        self.logger.info(
            "Updates montados bulkId=%s updates=%s",
            bulk_id,
            len(updates)
        )
        return updates

    def _build_status_update(self, report, id_registro, cedente):
        status = report.get("status", {})
        status_name = status.get("name")
        status_desc = status.get("description")
        done_at = _parse_infobip_datetime(report.get("doneAt"))
        sent_at = _parse_infobip_datetime(report.get("sentAt"))
        price = report.get("price") or {}
        error = report.get("error") or {}

        final_status = None
        final_status_desc = None

        if status_name in ("DELIVERED", "DELIVERED_TO_HANDSET"):
            final_status = "ENTREGUE"
            final_status_desc = _clip(status_desc, 255)
        elif (
            status_name
            and status_name.startswith("PENDING")
            and sent_at
            and sent_at < datetime.now(sent_at.tzinfo) - timedelta(days=5)
        ):
            final_status = "ERRO"
            final_status_desc = "Apos 5 dias, a mensagem foi considerada expirada e nao enviada"
        elif not (status_name and status_name.startswith("PENDING")):
            erro = (
                (report.get("error") or {}).get("description")
                or status_desc
                or "Status final nao entregue"
            )
            final_status = "ERRO"
            final_status_desc = _clip(erro, 50)

        return {
            "id_registro": id_registro,
            "id_mensagem": _clip(report.get("messageId"), 100),
            "to_number": _clip(report.get("to"), 20),
            "from_number": _clip(report.get("sender") or report.get("from"), 20),
            "status": _clip(status_name, 20),
            "status_desc": _clip(status_desc, 255),
            "error_name": _clip(error.get("name"), 50),
            "dt_envio": _to_sql_datetime(sent_at),
            "dt_entrega": _to_sql_datetime(done_at),
            "sms_count": report.get("messageCount") or report.get("smsCount"),
            "valor_infobip": price.get("pricePerMessage"),
            "moeda": _clip(price.get("currency"), 10),
            "final_status": final_status,
            "final_status_desc": final_status_desc,
        }

    def _flush_status_updates(self, updates):
        self.logger.info("Gravando updates de status no banco: itens=%s", len(updates))
        EnvioRepository.atualizar_status_infobip_batch(
            updates,
            batch_size=self.status_update_batch_tamanho,
            logger=self.logger
        )

