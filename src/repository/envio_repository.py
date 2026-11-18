import pyodbc
import re
import random
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from src.infrastructure.database.connection import get_connection


class EnvioRepository:
    LEITURA_ENVIO_VIEW = "infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA"
    LIMITE_TOTAL_SEMANAL_TABLE = "infobip.TB_RCS_LIMITE_TOTAL_SEMANAL"
    LIMITE_TOTAL_MENSAL_TABLE = "infobip.TB_RCS_LIMITE_TOTAL_MENSAL"

    @staticmethod
    def buscar_pendentes_aprovados(
        limit=None,
        logger=None
    ):
        if limit is None:
            select_top = ""
        else:
            select_top = "TOP (?)"

        query = f"""
                    DECLARE @inicio_dia DATETIME = CAST(GETDATE() AS DATE);
                    DECLARE @fim_dia DATETIME = DATEADD(DAY, 1, @inicio_dia);

                    SELECT {select_top}
                        ID,
                        CUSTOM_MESSAGE_ID,
                        ID_CEDENTE,
                        CEDENTE,
                        CANAL,
                        NM_USUARIO_ENVIO,
                        DT_INCLUSAO,
                        NUM_CLIENTE,
                        MENSAGEM,
                        MENSAGEM_TEMPLATE,
                        VARIAVEIS_JSON,
                        STATUS,
                        DT_ATUALIZACAO
                    FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
                    WHERE
                        STATUS IN ('PENDENTE__APROVADO', 'APROVADO__GERENCIA')
                        AND DT_INCLUSAO >= @inicio_dia
                        AND DT_INCLUSAO < @fim_dia
                    ORDER BY DT_INCLUSAO;
        """

        params = []
        if limit is not None:
            params.append(limit)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *params)
            rows = cursor.fetchall()
            if logger:
                logger.info("Pendentes aprovados carregados: %s", len(rows))
            return rows

    @staticmethod
    def buscar_pendentes_para_envio(
        limit=None,
        limite_pendentes_cedente=0,
        limite_pendentes_fila=0,
        ids_cedente_limite_especial=None,
        limite_pendentes_cedente_especial=0,
        limite_pendentes_fila_especial=0,
        permitir_pendentes=True,
        canal=None,
        logger=None
    ):
        ids_especiais = [int(x) for x in (ids_cedente_limite_especial or []) if x is not None]
        if ids_especiais:
            placeholders_especiais = ", ".join("?" for _ in ids_especiais)
            especial_condition = f"MAX(ID_CEDENTE) IN ({placeholders_especiais})"
        else:
            especial_condition = "1 = 0"

        if limit is None:
            select_top = ""
        else:
            select_top = "TOP (?)"

        query = f"""
                    DECLARE @inicio_hora DATETIME = DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0);
                    DECLARE @fim_hora DATETIME = DATEADD(HOUR, 1, @inicio_hora);
                    DECLARE @inicio_dia DATETIME = CAST(GETDATE() AS DATE);
                    DECLARE @fim_dia DATETIME = DATEADD(DAY, 1, @inicio_dia);

                    SELECT {select_top}
                        ID,
                        CUSTOM_MESSAGE_ID,
                        ID_CEDENTE,
                        CEDENTE,
                        CANAL,
                        NM_USUARIO_ENVIO,
                        DT_INCLUSAO,
                        NUM_CLIENTE,
                        MENSAGEM,
                        MENSAGEM_TEMPLATE,
                        VARIAVEIS_JSON,
                        STATUS,
                        DT_ATUALIZACAO
                    FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
                    WHERE
                        ? = 1
                        AND STATUS = 'PENDENTE'
                        AND STATUS <> 'RESERVADO'
                        AND (? IS NULL OR CANAL = ?)
                        AND DT_INCLUSAO >= @inicio_dia
                        AND DT_INCLUSAO < @fim_dia
                        AND CEDENTE NOT IN (
                            SELECT CEDENTE
                            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
                            WHERE DT_ENVIO >= @inicio_hora
                              AND DT_ENVIO < @fim_hora
                            GROUP BY CEDENTE
                            HAVING COUNT(*) >= CASE
                                WHEN {especial_condition} THEN ?
                                ELSE ?
                            END
                        )
                        AND CEDENTE NOT IN (
                            SELECT CEDENTE
                            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
                            WHERE STATUS = 'PENDENTE'
                              AND DT_INCLUSAO >= @inicio_dia
                              AND DT_INCLUSAO < @fim_dia
                            GROUP BY CEDENTE
                            HAVING COUNT(*) >= CASE
                                WHEN {especial_condition} THEN ?
                                ELSE ?
                            END
                        )
                    ORDER BY DT_INCLUSAO;
        """

        params = []
        if limit is not None:
            params.append(limit)
        params.extend([
            1 if permitir_pendentes else 0,
            canal,
            canal,
            *ids_especiais,
            limite_pendentes_cedente_especial,
            limite_pendentes_cedente,
            *ids_especiais,
            limite_pendentes_fila_especial,
            limite_pendentes_fila
        ])

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *params)
            rows = cursor.fetchall()
            if logger:
                logger.info("Pendentes carregados: %s", len(rows))
            return rows

    @staticmethod
    def marcar_reservado_batch(ids_registro, batch_size=1000, logger=None):
        if not ids_registro:
            return

        with get_connection() as conn:
            cursor = conn.cursor()
            for i in range(0, len(ids_registro), batch_size):
                chunk = ids_registro[i:i + batch_size]
                try:
                    cursor.execute("""
                        CREATE TABLE #tmp_reserva (
                            ID_REGISTRO INT NOT NULL
                        );
                        """)
                    cursor.fast_executemany = True
                    cursor.executemany(
                        "INSERT INTO #tmp_reserva (ID_REGISTRO) VALUES (?);",
                        [(rid,) for rid in chunk]
                    )
                    cursor.execute(
                        """
                        UPDATE m
                        SET
                            STATUS = 'RESERVADO',
                            DT_ATUALIZACAO = GETDATE()
                        FROM infobip.TB_ENVIO_RCS_MENSAGEM m
                        INNER JOIN #tmp_reserva t ON t.ID_REGISTRO = m.ID
                        WHERE m.STATUS IN ('PENDENTE', 'PENDENTE__APROVADO', 'APROVADO__GERENCIA');
                        """
                    )
                    if logger:
                        logger.info(
                            "Batch reserva: itens=%s rows_tb=%s",
                            len(chunk),
                            cursor.rowcount
                        )
                finally:
                    try:
                        cursor.execute("DROP TABLE #tmp_reserva;")
                    except Exception:
                        pass
            conn.commit()

    @staticmethod
    def marcar_nao_enviado_reservados_batch(ids_registro, status_desc=None, batch_size=1000, logger=None):
        if not ids_registro:
            return 0

        total_rows = 0
        with get_connection() as conn:
            cursor = conn.cursor()
            for i in range(0, len(ids_registro), batch_size):
                chunk = ids_registro[i:i + batch_size]
                try:
                    cursor.execute("""
                        CREATE TABLE #tmp_nao_enviado (
                            ID_REGISTRO INT NOT NULL
                        );
                        """)
                    cursor.fast_executemany = True
                    cursor.executemany(
                        "INSERT INTO #tmp_nao_enviado (ID_REGISTRO) VALUES (?);",
                        [(rid,) for rid in chunk]
                    )
                    cursor.execute(
                        """
                        UPDATE m
                        SET
                            STATUS = 'NAO_ENVIADO',
                            STATUS_DESC = COALESCE(?, 'Nao enviado: processo interrompido antes da confirmacao de envio'),
                            DT_ATUALIZACAO = GETDATE()
                        FROM infobip.TB_ENVIO_RCS_MENSAGEM m
                        INNER JOIN #tmp_nao_enviado t ON t.ID_REGISTRO = m.ID
                        WHERE m.STATUS = 'RESERVADO';
                        """,
                        status_desc
                    )
                    total_rows += cursor.rowcount
                    if logger:
                        logger.info(
                            "Batch nao_enviado_reservado: itens=%s rows_tb=%s",
                            len(chunk),
                            cursor.rowcount
                        )
                finally:
                    try:
                        cursor.execute("DROP TABLE #tmp_nao_enviado;")
                    except Exception:
                        pass
            conn.commit()
        return total_rows

    @staticmethod
    def atualizar_envio_batch(items, batch_size=500, logger=None):
        if not items:
            return

        def _clip(value, max_len):
            if value is None:
                return None
            text = str(value)
            return text if len(text) <= max_len else text[:max_len]

        with get_connection() as conn:
            cursor = conn.cursor()
            for i in range(0, len(items), batch_size):
                chunk = items[i:i + batch_size]
                try:
                    cursor.execute("""
                        CREATE TABLE #tmp_envio (
                            ID_REGISTRO INT NOT NULL,
                            STATUS NVARCHAR(20) NULL,
                            STATUS_DESC NVARCHAR(255) NULL,
                            ID_MENSAGEM NVARCHAR(100) NULL,
                            BULK_ID NVARCHAR(100) NULL,
                            CUSTOM_MESSAGE_ID NVARCHAR(100) NULL,
                            MENSAGEM NVARCHAR(MAX) NULL
                        );
                        """)
                    cursor.fast_executemany = True
                    cursor.setinputsizes([
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        (pyodbc.SQL_WVARCHAR, 0, 0),
                    ])
                    cursor.executemany("""
                        INSERT INTO #tmp_envio (
                            ID_REGISTRO,
                            STATUS,
                            STATUS_DESC,
                            ID_MENSAGEM,
                            BULK_ID,
                            CUSTOM_MESSAGE_ID,
                            MENSAGEM
                        ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, [
                            (
                                item.get('id_registro'),
                                _clip(item.get('status'), 20),
                                _clip(item.get('status_desc'), 255),
                                _clip(item.get('id_mensagem'), 100),
                                _clip(item.get('bulk_id'), 100),
                                _clip(item.get('custom_message_id'), 100),
                                item.get('mensagem')
                            )
                            for item in chunk
                        ])

                    cursor.execute("""
                        UPDATE m
                        SET
                            STATUS = COALESCE(t.STATUS, m.STATUS),
                            STATUS_DESC = COALESCE(t.STATUS_DESC, m.STATUS_DESC),
                            ID_MENSAGEM = COALESCE(t.ID_MENSAGEM, m.ID_MENSAGEM),
                            BULK_ID = COALESCE(t.BULK_ID, m.BULK_ID),
                            CUSTOM_MESSAGE_ID = COALESCE(t.CUSTOM_MESSAGE_ID, m.CUSTOM_MESSAGE_ID),
                            MENSAGEM = COALESCE(t.MENSAGEM, m.MENSAGEM),
                            DT_ENVIO = CASE
                                WHEN t.STATUS = 'ENVIADO' THEN COALESCE(m.DT_ENVIO, GETDATE())
                                ELSE m.DT_ENVIO
                            END,
                            DT_ATUALIZACAO = GETDATE()
                        FROM infobip.TB_ENVIO_RCS_MENSAGEM m
                        INNER JOIN #tmp_envio t ON t.ID_REGISTRO = m.ID;
                        """)
                    rows_m = cursor.rowcount

                    if logger:
                        logger.info("Batch envio update: itens=%s rows_tb=%s", len(chunk), rows_m)
                        if rows_m == 0:
                            logger.warning("Batch envio update: 0 linhas em TB_ENVIO_RCS_MENSAGEM")
                except Exception:
                    if logger:
                        logger.exception("Erro ao atualizar envio em batch (itens=%s)", len(chunk))
                    raise
                finally:
                    try:
                        cursor.execute("DROP TABLE #tmp_envio;")
                    except Exception:
                        pass
            conn.commit()
    def buscar_cedentes_com_envios_ultima_hora_acima(
        limite,
        ids_cedente_limite_especial=None,
        limite_especial=None,
        logger=None
    ):
        ids_especiais = [int(x) for x in (ids_cedente_limite_especial or []) if x is not None]
        if ids_especiais:
            placeholders_especiais = ", ".join("?" for _ in ids_especiais)
            especial_condition = f"MAX(ID_CEDENTE) IN ({placeholders_especiais})"
        else:
            especial_condition = "1 = 0"

        query = """
            DECLARE @inicio DATETIME = DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0);
            DECLARE @fim DATETIME = DATEADD(HOUR, 1, @inicio);

            SELECT
                CEDENTE,
                COUNT(*) AS TOTAL
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE DT_ENVIO >= @inicio
              AND DT_ENVIO < @fim
            GROUP BY CEDENTE
            HAVING COUNT(*) > CASE
                WHEN """ + especial_condition + """ THEN ?
                ELSE ?
            END
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *ids_especiais, limite_especial, limite)
            rows = cursor.fetchall()
            if logger:
                logger.info(
                    "Cedentes com envios acima do limite: %s",
                    len(rows)
                )
            return rows

    @staticmethod
    def buscar_cedentes_com_pendentes_acima(
        limite,
        ids_cedente_limite_especial=None,
        limite_especial=None,
        logger=None
    ):
        ids_especiais = [int(x) for x in (ids_cedente_limite_especial or []) if x is not None]
        if ids_especiais:
            placeholders_especiais = ", ".join("?" for _ in ids_especiais)
            especial_condition = f"MAX(ID_CEDENTE) IN ({placeholders_especiais})"
        else:
            especial_condition = "1 = 0"

        query = """
            DECLARE @inicio_dia DATETIME = CAST(GETDATE() AS DATE);
            DECLARE @fim_dia DATETIME = DATEADD(DAY, 1, @inicio_dia);

            SELECT
                CEDENTE,
                COUNT(*) AS TOTAL
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE STATUS = 'PENDENTE'
              AND DT_INCLUSAO >= @inicio_dia
              AND DT_INCLUSAO < @fim_dia
            GROUP BY CEDENTE
            HAVING COUNT(*) > CASE
                WHEN """ + especial_condition + """ THEN ?
                ELSE ?
            END
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *ids_especiais, limite_especial, limite)
            rows = cursor.fetchall()
            if logger:
                logger.info(
                    "Cedentes com pendentes acima do limite: %s",
                    len(rows)
                )
            return rows

    @staticmethod
    def contar_envios_ultima_hora_total():
        query = """
            DECLARE @inicio DATETIME = DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0);
            DECLARE @fim DATETIME = DATEADD(HOUR, 1, @inicio);

            SELECT COUNT(*) AS TOTAL
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE DT_ENVIO >= @inicio
              AND DT_ENVIO < @fim
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            return row.TOTAL if row else 0

    @staticmethod
    def existe_pendente_elegivel_por_cedente(id_cedente, canal=None):
        query = """
            DECLARE @inicio_dia DATETIME = CAST(GETDATE() AS DATE);
            DECLARE @fim_dia DATETIME = DATEADD(DAY, 1, @inicio_dia);

            SELECT TOP (1) 1
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE ID_CEDENTE = ?
              AND STATUS IN ('PENDENTE', 'PENDENTE__APROVADO', 'APROVADO__GERENCIA')
              AND DT_INCLUSAO >= @inicio_dia
              AND DT_INCLUSAO < @fim_dia
              AND (? IS NULL OR CANAL = ?)
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, id_cedente, canal, canal)
            return cursor.fetchone() is not None

    @staticmethod
    def bloquear_pendentes_todos(status_desc=None):
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS = 'PENDENTE_BLOQUEADO',
                STATUS_DESC = ?
            WHERE STATUS = 'PENDENTE'
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc)
            conn.commit()

    @staticmethod
    def bloquear_pendentes_por_cedente(cedente, status_desc=None):
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS = 'PENDENTE_BLOQUEADO',
                STATUS_DESC = ?
            WHERE CEDENTE = ?
              AND STATUS = 'PENDENTE'
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc, cedente)
            conn.commit()

    @staticmethod
    def bloquear_pendentes_por_cedente_todos_status(cedente, status_desc=None):
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS = 'PENDENTE_BLOQUEADO',
                STATUS_DESC = ?
            WHERE UPPER(LTRIM(RTRIM(CEDENTE))) = UPPER(LTRIM(RTRIM(?)))
              AND STATUS IN ('PENDENTE', 'PENDENTE__APROVADO', 'APROVADO__GERENCIA')
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc, cedente)
            rows = cursor.rowcount
            conn.commit()
            return rows

    @staticmethod
    def bloquear_pendentes_excedentes_semana(status_desc=None, incluir_aprovado=False, logger=None):
        query = """
            SET NOCOUNT ON;
            SET DEADLOCK_PRIORITY LOW;
            SET LOCK_TIMEOUT 5000;

            DECLARE @hoje DATE = CAST(GETDATE() AS DATE);
            DECLARE @inicio_semana DATE = DATEADD(DAY, -(DATEDIFF(DAY, '19000101', @hoje) % 7), @hoje);
            DECLARE @fim_semana DATE = DATEADD(DAY, 7, @inicio_semana);
            DECLARE @inicio_mes DATE = DATEFROMPARTS(YEAR(@hoje), MONTH(@hoje), 1);
            DECLARE @inicio_semana_mes DATE = DATEADD(DAY, -(DATEDIFF(DAY, '19000101', @inicio_mes) % 7), @inicio_mes);
            DECLARE @inicio_acumulo_mes DATE =
                CASE
                    WHEN @inicio_semana_mes < @inicio_mes THEN DATEADD(DAY, 7, @inicio_semana_mes)
                    ELSE @inicio_semana_mes
                END;
            DECLARE @semanas_anteriores INT =
                CASE
                    WHEN @inicio_semana <= @inicio_acumulo_mes THEN 0
                    ELSE DATEDIFF(DAY, @inicio_acumulo_mes, @inicio_semana) / 7
                END;
            DECLARE @limite_semanal_total INT;
            DECLARE @total_enviado_global INT;
            DECLARE @total_enviado_anterior_mes_global INT;

            SELECT
                CEDENTE,
                MAX(ID_CEDENTE) AS ID_CEDENTE,
                MAX(LIMITE_MENSAL) AS LIMITE_SEMANAL
            INTO #limites_cedente
            FROM infobip.TB_CEDENTE_RCS_LIMITE_SEMANAL WITH (NOLOCK)
            WHERE ATIVO = 1
              AND UPPER(LTRIM(RTRIM(CEDENTE))) <> 'TOTAL'
            GROUP BY CEDENTE;

            CREATE CLUSTERED INDEX IX_limites_cedente ON #limites_cedente (CEDENTE);

            SELECT @limite_semanal_total = MAX(LIMITE_TOTAL)
            FROM infobip.TB_RCS_LIMITE_TOTAL_SEMANAL WITH (NOLOCK)
            WHERE ATIVO = 1;

            SELECT
                CEDENTE,
                SUM(CASE WHEN DT_ENVIO >= @inicio_semana AND DT_ENVIO < @fim_semana THEN 1 ELSE 0 END) AS TOTAL_ENVIADO,
                SUM(CASE WHEN DT_ENVIO >= @inicio_acumulo_mes AND DT_ENVIO < @inicio_semana THEN 1 ELSE 0 END) AS TOTAL_ENVIADO_ANTERIOR_MES
            INTO #enviados_cedente
            FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
            WHERE DT_ENVIO >= @inicio_acumulo_mes
              AND DT_ENVIO < @fim_semana
            GROUP BY CEDENTE;

            CREATE CLUSTERED INDEX IX_enviados_cedente ON #enviados_cedente (CEDENTE);

            SELECT
                @total_enviado_global = ISNULL(SUM(TOTAL_ENVIADO), 0),
                @total_enviado_anterior_mes_global = ISNULL(SUM(TOTAL_ENVIADO_ANTERIOR_MES), 0)
            FROM #enviados_cedente;

            WITH pendentes AS (
                SELECT
                    m.ID,
                    m.ID_CEDENTE,
                    m.CEDENTE,
                    lc.LIMITE_SEMANAL,
                    ISNULL(ec.TOTAL_ENVIADO, 0) AS TOTAL_ENVIADO,
                    CASE
                        WHEN (@semanas_anteriores * lc.LIMITE_SEMANAL) - ISNULL(ec.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                            THEN (@semanas_anteriores * lc.LIMITE_SEMANAL) - ISNULL(ec.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                        ELSE 0
                    END AS NAO_UTILIZADO,
                    @limite_semanal_total AS LIMITE_SEMANAL_TOTAL,
                    @total_enviado_global AS TOTAL_ENVIADO_GLOBAL,
                    CASE
                        WHEN @limite_semanal_total IS NULL THEN 0
                        WHEN (@semanas_anteriores * @limite_semanal_total) - @total_enviado_anterior_mes_global > 0
                            THEN (@semanas_anteriores * @limite_semanal_total) - @total_enviado_anterior_mes_global
                        ELSE 0
                    END AS NAO_UTILIZADO_GLOBAL,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.CEDENTE
                        ORDER BY m.DT_INCLUSAO, m.ID
                    ) AS RN,
                    ROW_NUMBER() OVER (
                        ORDER BY m.DT_INCLUSAO, m.ID
                    ) AS RN_GLOBAL
                FROM infobip.TB_ENVIO_RCS_MENSAGEM m WITH (READPAST)
                INNER JOIN #limites_cedente lc ON lc.CEDENTE = m.CEDENTE
                LEFT JOIN #enviados_cedente ec ON ec.CEDENTE = m.CEDENTE
                WHERE m.STATUS IN ({status_list})
                  AND m.DT_INCLUSAO >= @inicio_semana
                  AND m.DT_INCLUSAO < @fim_semana
            )
            SELECT *
            INTO #pendentes_semana
            FROM pendentes;

            CREATE CLUSTERED INDEX IX_pendentes_semana ON #pendentes_semana (ID);

            UPDATE m
            SET
                STATUS = 'PENDENTE_BLOQUEADO',
                STATUS_DESC = ?
            OUTPUT
                inserted.ID_CEDENTE,
                inserted.CEDENTE
            FROM infobip.TB_ENVIO_RCS_MENSAGEM m WITH (UPDLOCK, READPAST, ROWLOCK)
            INNER JOIN #pendentes_semana p ON p.ID = m.ID
            WHERE m.STATUS IN ({status_list})
              AND (((p.LIMITE_SEMANAL + p.NAO_UTILIZADO) - p.TOTAL_ENVIADO) <= 0
               OR p.RN > ((p.LIMITE_SEMANAL + p.NAO_UTILIZADO) - p.TOTAL_ENVIADO)
               OR (
                    p.LIMITE_SEMANAL_TOTAL IS NOT NULL
                    AND (
                        ((p.LIMITE_SEMANAL_TOTAL + p.NAO_UTILIZADO_GLOBAL) - p.TOTAL_ENVIADO_GLOBAL) <= 0
                        OR p.RN_GLOBAL > ((p.LIMITE_SEMANAL_TOTAL + p.NAO_UTILIZADO_GLOBAL) - p.TOTAL_ENVIADO_GLOBAL)
                    )
               ));
        """

        status_list = "'PENDENTE','PENDENTE__APROVADO'" if incluir_aprovado else "'PENDENTE'"
        query = query.format(status_list=status_list)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc)
            rows = cursor.fetchall()
            conn.commit()
            bloqueados = len(rows)
            if logger:
                logger.info(
                    "Pendentes bloqueados por limite semanal: %s",
                    bloqueados
                )
            return rows

    @staticmethod
    def bloquear_pendentes_excedentes_mes(status_desc=None, incluir_aprovado=False, logger=None):
        query = """
            SET NOCOUNT ON;
            SET DEADLOCK_PRIORITY LOW;
            SET LOCK_TIMEOUT 5000;

            DECLARE @inicio DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1);
            DECLARE @fim DATE = DATEADD(MONTH, 1, @inicio);
            DECLARE @limite_mensal_total INT;
            DECLARE @total_enviado_global INT;

            SELECT
                CEDENTE,
                MAX(ID_CEDENTE) AS ID_CEDENTE,
                MAX(LIMITE_MENSAL) AS LIMITE_MENSAL
            INTO #limites_cedente_mes
            FROM infobip.TB_CEDENTE_RCS_LIMITE_MENSAL WITH (NOLOCK)
            WHERE ATIVO = 1
              AND UPPER(LTRIM(RTRIM(CEDENTE))) <> 'TOTAL'
            GROUP BY CEDENTE;

            CREATE CLUSTERED INDEX IX_limites_cedente_mes ON #limites_cedente_mes (CEDENTE);

            SELECT @limite_mensal_total = MAX(LIMITE_TOTAL)
            FROM infobip.TB_RCS_LIMITE_TOTAL_MENSAL WITH (NOLOCK)
            WHERE ATIVO = 1;

            SELECT
                CEDENTE,
                COUNT(*) AS TOTAL_ENVIADO
            INTO #enviados_cedente_mes
            FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
            WHERE DT_ENVIO >= @inicio
              AND DT_ENVIO < @fim
            GROUP BY CEDENTE;

            CREATE CLUSTERED INDEX IX_enviados_cedente_mes ON #enviados_cedente_mes (CEDENTE);

            SELECT @total_enviado_global = ISNULL(SUM(TOTAL_ENVIADO), 0)
            FROM #enviados_cedente_mes;

            WITH pendentes AS (
                SELECT
                    m.ID,
                    m.ID_CEDENTE,
                    m.CEDENTE,
                    lc.LIMITE_MENSAL,
                    ISNULL(ec.TOTAL_ENVIADO, 0) AS TOTAL_ENVIADO,
                    @limite_mensal_total AS LIMITE_MENSAL_TOTAL,
                    @total_enviado_global AS TOTAL_ENVIADO_GLOBAL,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.CEDENTE
                        ORDER BY m.DT_INCLUSAO, m.ID
                    ) AS RN,
                    ROW_NUMBER() OVER (
                        ORDER BY m.DT_INCLUSAO, m.ID
                    ) AS RN_GLOBAL
                FROM infobip.TB_ENVIO_RCS_MENSAGEM m WITH (READPAST)
                INNER JOIN #limites_cedente_mes lc ON lc.CEDENTE = m.CEDENTE
                LEFT JOIN #enviados_cedente_mes ec ON ec.CEDENTE = m.CEDENTE
                WHERE m.STATUS IN ({status_list})
                  AND m.DT_INCLUSAO >= @inicio
                  AND m.DT_INCLUSAO < @fim
            )
            SELECT *
            INTO #pendentes_mes
            FROM pendentes;

            CREATE CLUSTERED INDEX IX_pendentes_mes ON #pendentes_mes (ID);

            UPDATE m
            SET
                STATUS = 'PENDENTE_BLOQUEADO',
                STATUS_DESC = ?
            OUTPUT
                inserted.ID_CEDENTE,
                inserted.CEDENTE
            FROM infobip.TB_ENVIO_RCS_MENSAGEM m WITH (UPDLOCK, READPAST, ROWLOCK)
            INNER JOIN #pendentes_mes p ON p.ID = m.ID
            WHERE m.STATUS IN ({status_list})
              AND ((p.LIMITE_MENSAL - p.TOTAL_ENVIADO) <= 0
               OR p.RN > (p.LIMITE_MENSAL - p.TOTAL_ENVIADO)
               OR (
                    p.LIMITE_MENSAL_TOTAL IS NOT NULL
                    AND (
                        (p.LIMITE_MENSAL_TOTAL - p.TOTAL_ENVIADO_GLOBAL) <= 0
                        OR p.RN_GLOBAL > (p.LIMITE_MENSAL_TOTAL - p.TOTAL_ENVIADO_GLOBAL)
                    )
               ));
        """

        status_list = "'PENDENTE','PENDENTE__APROVADO'" if incluir_aprovado else "'PENDENTE'"
        query = query.format(status_list=status_list)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc)
            rows = cursor.fetchall()
            conn.commit()
            bloqueados = len(rows)
            if logger:
                logger.info(
                    "Pendentes bloqueados por limite mensal: %s",
                    bloqueados
                )
            return rows

    @staticmethod
    def bloquear_pendentes_fora_padrao_semanal(
        status_desc=None,
        incluir_aprovado=False,
        logger=None
    ):
        query = """
            WITH fora_padrao AS (
                SELECT
                    m.ID,
                    m.ID_CEDENTE,
                    m.CEDENTE
                FROM infobip.TB_ENVIO_RCS_MENSAGEM m WITH (UPDLOCK, READPAST, ROWLOCK)
                LEFT JOIN infobip.TB_CEDENTE_RCS_LIMITE_SEMANAL l WITH (NOLOCK)
                    ON l.ATIVO = 1
                   AND l.ID_CEDENTE = m.ID_CEDENTE
                   AND l.CEDENTE = m.CEDENTE
                WHERE m.STATUS IN ({status_list})
                  AND l.ID_CEDENTE IS NULL
            )
            UPDATE m
            SET
                STATUS = 'PENDENTE_BLOQUEADO',
                STATUS_DESC = ?
            OUTPUT
                inserted.ID_CEDENTE,
                inserted.CEDENTE
            FROM infobip.TB_ENVIO_RCS_MENSAGEM m
            INNER JOIN fora_padrao f ON f.ID = m.ID;
        """

        status_list = "'PENDENTE'" if incluir_aprovado else "'PENDENTE'"
        query = query.format(status_list=status_list)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc)
            rows = cursor.fetchall()
            conn.commit()
            bloqueados = len(rows)
            if logger:
                logger.info(
                    "Pendentes bloqueados por fora do padrao semanal: %s",
                    bloqueados
                )
            return rows

    @staticmethod
    def bloquear_pendentes_fora_padrao_mensal(
        status_desc=None,
        incluir_aprovado=False,
        logger=None
    ):
        query = """
            WITH fora_padrao AS (
                SELECT
                    m.ID,
                    m.ID_CEDENTE,
                    m.CEDENTE
                FROM infobip.TB_ENVIO_RCS_MENSAGEM m WITH (UPDLOCK, READPAST, ROWLOCK)
                LEFT JOIN infobip.TB_CEDENTE_RCS_LIMITE_MENSAL l WITH (NOLOCK)
                    ON l.ATIVO = 1
                   AND l.ID_CEDENTE = m.ID_CEDENTE
                   AND l.CEDENTE = m.CEDENTE
                WHERE m.STATUS IN ({status_list})
                  AND l.ID_CEDENTE IS NULL
            )
            UPDATE m
            SET
                STATUS = 'PENDENTE_BLOQUEADO',
                STATUS_DESC = ?
            OUTPUT
                inserted.ID_CEDENTE,
                inserted.CEDENTE
            FROM infobip.TB_ENVIO_RCS_MENSAGEM m
            INNER JOIN fora_padrao f ON f.ID = m.ID;
        """

        status_list = "'PENDENTE','PENDENTE__APROVADO'" if incluir_aprovado else "'PENDENTE'"
        query = query.format(status_list=status_list)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc)
            rows = cursor.fetchall()
            conn.commit()
            bloqueados = len(rows)
            if logger:
                logger.info(
                    "Pendentes bloqueados por fora do padrao mensal: %s",
                    bloqueados
                )
            return rows

    @staticmethod
    def atualizar_custom_message_id(id_registro, custom_message_id):
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                CUSTOM_MESSAGE_ID = ?
            WHERE ID = ?
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, custom_message_id, id_registro)
            conn.commit()

    @staticmethod
    def atualizar_mensagem_final(id_registro, mensagem):
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                MENSAGEM = ?
            WHERE ID = ?
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, mensagem, id_registro)
            conn.commit()

    @staticmethod
    def buscar_enviados_pendentes_status(
        limit=10000,
        min_sent_age_minutes=120,
        min_interval_minutes=60,
        logger=None
    ):
        with get_connection() as conn:
            cursor = conn.cursor()
            query_enviados = """
                SELECT TOP (?)
                    ID,
                    ID_MENSAGEM,
                    CEDENTE,
                    STATUS_DESC,
                    BULK_ID,
                    DT_ENVIO
                FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
                WHERE STATUS = 'ENVIADO'
                  AND ID_MENSAGEM IS NOT NULL
                  AND DT_ENVIO IS NOT NULL
                  AND DT_ENVIO < DATEADD(MINUTE, -?, GETDATE())
                  AND (
                      DT_ATUALIZACAO IS NULL
                      OR DT_ATUALIZACAO < DATEADD(MINUTE, -?, GETDATE())
                  )
                ORDER BY
                    ISNULL(DT_ATUALIZACAO, '19000101'),
                    DT_ENVIO,
                    ID
            """
            cursor.execute(query_enviados, limit, min_sent_age_minutes, min_interval_minutes)
            rows = list(cursor.fetchall())

            restante = max(0, limit - len(rows))
            if restante > 0:
                query_pendentes = """
                    SELECT TOP (?)
                        ID,
                        ID_MENSAGEM,
                        CEDENTE,
                        STATUS_DESC,
                        BULK_ID,
                        DT_ENVIO
                    FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
                    WHERE STATUS LIKE 'PENDING%'
                      AND ID_MENSAGEM IS NOT NULL
                      AND DT_ENVIO IS NOT NULL
                      AND DT_ENVIO < DATEADD(MINUTE, -?, GETDATE())
                      AND (
                          DT_ATUALIZACAO IS NULL
                          OR DT_ATUALIZACAO < DATEADD(MINUTE, -?, GETDATE())
                      )
                    ORDER BY
                        ISNULL(DT_ATUALIZACAO, '19000101'),
                        DT_ENVIO,
                        ID
                """
                cursor.execute(query_pendentes, restante, min_sent_age_minutes, min_interval_minutes)
                rows.extend(cursor.fetchall())

            if logger:
                logger.info(
                    "Enviados pendentes de status: %s",
                    len(rows)
                )
            return rows

    @staticmethod
    def expirar_pendentes_antigos(logger=None):
        select_batch_size = 2000
        update_batch_size = 500
        max_tentativas = 5
        total_rows = 0

        query_select = """
            SELECT TOP (?)
                ID
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE (STATUS LIKE 'PENDING%' OR STATUS = 'SEM_RETORNO')
              AND DT_ENVIO IS NOT NULL
              AND DT_ENVIO < DATEADD(DAY, -5, GETDATE())
            ORDER BY DT_ENVIO, ID
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET DEADLOCK_PRIORITY LOW")
            cursor.execute("SET LOCK_TIMEOUT 2000")

            while True:
                cursor.execute(query_select, select_batch_size)
                ids = [row.ID for row in cursor.fetchall()]
                if not ids:
                    break

                if logger:
                    logger.info("Pendentes antigos carregados em cache para expiracao: %s", len(ids))

                for start in range(0, len(ids), update_batch_size):
                    chunk = ids[start:start + update_batch_size]
                    placeholders = ",".join("?" for _ in chunk)
                    query_update = f"""
                        UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
                        SET
                            STATUS = 'ERRO',
                            STATUS_DESC = CASE
                                WHEN STATUS = 'SEM_RETORNO'
                                    THEN 'A Infobip nao informou nenhuma informacao sobre este ID mensagem apos 5 dias'
                                ELSE 'Apos 5 dias, a mensagem foi considerada expirada e nao enviada'
                            END,
                            DT_ATUALIZACAO = GETDATE()
                        WHERE ID IN ({placeholders})
                    """

                    for tentativa in range(1, max_tentativas + 1):
                        try:
                            cursor.execute(query_update, *chunk)
                            rows = cursor.rowcount
                            conn.commit()
                            break
                        except pyodbc.Error as exc:
                            msg = str(exc).lower()
                            if "1205" in msg or "deadlock" in msg or "timeout" in msg:
                                conn.rollback()
                                if logger:
                                    logger.warning(
                                        "Deadlock/timeout expirando pendentes antigos: tentativa=%s/%s lote=%s",
                                        tentativa,
                                        max_tentativas,
                                        len(chunk)
                                    )
                                if tentativa < max_tentativas:
                                    time.sleep(0.5 + random.random())
                                    continue
                            raise

                    total_rows += rows
                    if logger:
                        logger.info(
                            "Pendentes expirados automaticamente: lote=%s total=%s",
                            rows,
                            total_rows
                        )

        return total_rows

    @staticmethod
    def marcar_sem_retorno_antigos(logger=None):
        select_batch_size = 2000
        update_batch_size = 500
        max_tentativas = 5
        total_rows = 0

        query_select = """
            SELECT TOP (?)
                ID
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE (STATUS = 'ENVIADO' OR STATUS LIKE 'PENDING%')
              AND ID_MENSAGEM IS NOT NULL
              AND DT_ENVIO IS NOT NULL
              AND DT_ENVIO < DATEADD(DAY, -2, GETDATE())
            ORDER BY DT_ENVIO, ID
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET DEADLOCK_PRIORITY LOW")
            cursor.execute("SET LOCK_TIMEOUT 2000")

            while True:
                cursor.execute(query_select, select_batch_size)
                ids = [row.ID for row in cursor.fetchall()]
                if not ids:
                    break

                if logger:
                    logger.info("Casos >2 dias carregados em cache para SEM_RETORNO: %s", len(ids))

                for start in range(0, len(ids), update_batch_size):
                    chunk = ids[start:start + update_batch_size]
                    placeholders = ",".join("?" for _ in chunk)
                    query_update = f"""
                        UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
                        SET
                            STATUS = 'SEM_RETORNO',
                            STATUS_DESC = 'A Infobip nao tem retorno de status para este ID mensagem apos 2 dias',
                            DT_ATUALIZACAO = GETDATE()
                        WHERE ID IN ({placeholders})
                    """

                    for tentativa in range(1, max_tentativas + 1):
                        try:
                            cursor.execute(query_update, *chunk)
                            rows = cursor.rowcount
                            conn.commit()
                            break
                        except pyodbc.Error as exc:
                            msg = str(exc).lower()
                            if "1205" in msg or "deadlock" in msg or "timeout" in msg:
                                conn.rollback()
                                if logger:
                                    logger.warning(
                                        "Deadlock/timeout marcando >2 dias como SEM_RETORNO: tentativa=%s/%s lote=%s",
                                        tentativa,
                                        max_tentativas,
                                        len(chunk)
                                    )
                                if tentativa < max_tentativas:
                                    time.sleep(0.5 + random.random())
                                    continue
                            raise

                    total_rows += rows
                    if logger:
                        logger.info(
                            "Casos >2 dias marcados como SEM_RETORNO: lote=%s total=%s",
                            rows,
                            total_rows
                        )

        return total_rows

    @staticmethod
    def marcar_consulta_status_batch(ids_registro):
        if not ids_registro:
            return

        placeholders = ",".join("?" for _ in ids_registro)
        query = f"""
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET DT_ATUALIZACAO = GETDATE()
            WHERE ID IN ({placeholders})
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *ids_registro)
            conn.commit()

    @staticmethod
    def marcar_sem_retorno_status_batch(ids_registro):
        if not ids_registro:
            return

        placeholders = ",".join("?" for _ in ids_registro)
        query = f"""
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS = 'SEM_RETORNO',
                STATUS_DESC = CONCAT(
                    'A Infobip nao tem retorno de status para este ID mensagem',
                    ' (tentativa ',
                    CASE
                        WHEN STATUS = 'SEM_RETORNO'
                         AND CHARINDEX('tentativa ', STATUS_DESC) > 0
                         AND CHARINDEX('/3', STATUS_DESC) > CHARINDEX('tentativa ', STATUS_DESC)
                            THEN ISNULL(
                                TRY_CONVERT(
                                    INT,
                                    SUBSTRING(
                                        STATUS_DESC,
                                        CHARINDEX('tentativa ', STATUS_DESC) + LEN('tentativa '),
                                        CHARINDEX('/3', STATUS_DESC) - (CHARINDEX('tentativa ', STATUS_DESC) + LEN('tentativa '))
                                    )
                                ),
                                0
                            ) + 1
                        ELSE 1
                    END,
                    '/3)',
                    CASE
                        WHEN CHARINDEX('tentativa bulk ', ISNULL(STATUS_DESC, '')) > 0
                         AND CHARINDEX('/2', ISNULL(STATUS_DESC, '')) > CHARINDEX('tentativa bulk ', ISNULL(STATUS_DESC, ''))
                            THEN CONCAT(
                                ' | reconsulta individual apos 1 dia',
                                ' (tentativa bulk ',
                                ISNULL(
                                    TRY_CONVERT(
                                        INT,
                                        SUBSTRING(
                                            STATUS_DESC,
                                            CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '),
                                            CHARINDEX('/2', STATUS_DESC) - (CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '))
                                        )
                                    ),
                                    0
                                ),
                                '/2)'
                            )
                        ELSE ''
                    END
                ),
                DT_ATUALIZACAO = GETDATE()
            WHERE ID IN ({placeholders})
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *ids_registro)
            conn.commit()

    @staticmethod
    def contar_enviados_pendentes_status():
        query = """
            SELECT COUNT(*) AS TOTAL
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE (
                STATUS = 'ENVIADO'
                OR STATUS LIKE 'PENDING%'
            )
              AND ID_MENSAGEM IS NOT NULL
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            return row.TOTAL if row else 0

    @staticmethod
    def buscar_bulk_ids_pendentes_status(
        limit=10000,
        min_sent_age_minutes=120,
        min_interval_minutes=60,
        logger=None
    ):
        query = """
            SELECT TOP (?)
                BULK_ID AS BULK_ID,
                COUNT(*) AS TOTAL
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE (
                STATUS = 'ENVIADO'
                OR STATUS LIKE 'PENDING%'
            )
              AND ID_MENSAGEM IS NOT NULL
              AND BULK_ID IS NOT NULL
              AND DT_ENVIO IS NOT NULL
              AND DT_ENVIO < DATEADD(MINUTE, -?, GETDATE())
              AND (
                  CASE
                      WHEN CHARINDEX('tentativa bulk ', STATUS_DESC) > 0
                       AND CHARINDEX('/2', STATUS_DESC) > CHARINDEX('tentativa bulk ', STATUS_DESC)
                          THEN TRY_CONVERT(
                              INT,
                              SUBSTRING(
                                  STATUS_DESC,
                                  CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '),
                                  CHARINDEX('/2', STATUS_DESC) - (CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '))
                              )
                          )
                      ELSE 0
                  END
              ) < 2
              AND (
                  DT_ATUALIZACAO IS NULL
                  OR DT_ATUALIZACAO < DATEADD(MINUTE, -?, GETDATE())
              )
            GROUP BY BULK_ID
            ORDER BY COUNT(*) DESC
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, limit, min_sent_age_minutes, min_interval_minutes)
            rows = cursor.fetchall()
            if logger:
                logger.info(
                    "BulkIds pendentes de status: %s",
                    len(rows)
                )
            return rows

    @staticmethod
    def marcar_tentativa_bulk_batch(ids_registro):
        if not ids_registro:
            return

        placeholders = ",".join("?" for _ in ids_registro)
        query = f"""
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS_DESC = CONCAT(
                    CASE
                        WHEN CHARINDEX(' (tentativa bulk ', STATUS_DESC) > 0
                            THEN LEFT(STATUS_DESC, CHARINDEX(' (tentativa bulk ', STATUS_DESC) - 1)
                        WHEN CHARINDEX(' | reconsulta individual apos 1 dia', STATUS_DESC) > 0
                            THEN LEFT(STATUS_DESC, CHARINDEX(' | reconsulta individual apos 1 dia', STATUS_DESC) - 1)
                        ELSE ISNULL(STATUS_DESC, 'Aguardando retorno de status por bulkId')
                    END,
                    CASE
                        WHEN (
                            CASE
                                WHEN CHARINDEX('tentativa bulk ', STATUS_DESC) > 0
                                 AND CHARINDEX('/2', STATUS_DESC) > CHARINDEX('tentativa bulk ', STATUS_DESC)
                                    THEN ISNULL(
                                        TRY_CONVERT(
                                            INT,
                                            SUBSTRING(
                                                STATUS_DESC,
                                                CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '),
                                                CHARINDEX('/2', STATUS_DESC) - (CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '))
                                            )
                                        ),
                                        0
                                    ) + 1
                                ELSE 1
                            END
                        ) >= 2
                            THEN ' | reconsulta individual apos 1 dia'
                        ELSE ''
                    END,
                    ' (tentativa bulk ',
                    CASE
                        WHEN CHARINDEX('tentativa bulk ', STATUS_DESC) > 0
                         AND CHARINDEX('/2', STATUS_DESC) > CHARINDEX('tentativa bulk ', STATUS_DESC)
                            THEN ISNULL(
                                TRY_CONVERT(
                                    INT,
                                    SUBSTRING(
                                        STATUS_DESC,
                                        CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '),
                                        CHARINDEX('/2', STATUS_DESC) - (CHARINDEX('tentativa bulk ', STATUS_DESC) + LEN('tentativa bulk '))
                                    )
                                ),
                                0
                            ) + 1
                        ELSE 1
                    END,
                    '/2)'
                ),
                DT_ATUALIZACAO = GETDATE()
            WHERE ID IN ({placeholders})
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *ids_registro)
            conn.commit()

    @staticmethod
    def buscar_ids_por_message_ids(message_ids):
        if not message_ids:
            return []
        placeholders = ",".join("?" for _ in message_ids)
        query = f"""
            SELECT
                ID,
                ID_MENSAGEM,
                CEDENTE
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE ID_MENSAGEM IN ({placeholders})
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *message_ids)
            return cursor.fetchall()

    @staticmethod
    def marcar_enviado(id_registro, id_mensagem):
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                ID_MENSAGEM = ?,
                STATUS = 'ENVIADO',
                STATUS_DESC = 'pending',
                DT_ENVIO = GETDATE(),
            WHERE ID = ?
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, id_mensagem, id_registro)
            conn.commit()

    @staticmethod
    def atualizar_status(id_registro, status_desc=None, dt_entrega=None):
        if status_desc is not None:
            status_desc = str(status_desc)[:255]
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS_DESC = ?,
                DT_ENTREGA = COALESCE(?, DT_ENTREGA),
                DT_ATUALIZACAO = GETDATE()
            WHERE ID = ?
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_desc, dt_entrega, id_registro)
            conn.commit()

    @staticmethod
    def atualizar_status_infobip_batch(items, batch_size=100, logger=None):
        if not items:
            return

        def _clip(value, max_len):
            if value is None:
                return None
            text = str(value)
            return text if len(text) <= max_len else text[:max_len]
        def _to_int(value):
            if value is None or value == "":
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        def _to_decimal(value):
            if value is None or value == "":
                return None
            if isinstance(value, Decimal):
                try:
                    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                except InvalidOperation:
                    return value
            if isinstance(value, (int, float)):
                value = Decimal(str(value))
                return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            text = str(value).strip()
            if not text:
                return None
            # Keep only digits, comma, dot, minus
            text = re.sub(r"[^\d,.\-]", "", text)
            if not text:
                return None
            # Normalize decimal separator
            if "," in text and "." in text:
                text = text.replace(".", "").replace(",", ".")
            elif "," in text:
                text = text.replace(",", ".")
            try:
                value = Decimal(text)
                return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            except (InvalidOperation, ValueError):
                return None

        def _to_decimal_str(value):
            dec = _to_decimal(value)
            if dec is None:
                return None
            # Bind as string to avoid pyodbc precision loss
            return f"{dec:.6f}"

        with get_connection() as conn:
            cursor = conn.cursor()
            for i in range(0, len(items), batch_size):
                chunk = items[i:i + batch_size]
                try:
                    cursor.execute(
                        """
                        CREATE TABLE #tmp_status (
                            ID_REGISTRO INT NOT NULL,
                            ID_MENSAGEM NVARCHAR(100) NULL,
                            TO_NUMBER NVARCHAR(20) NULL,
                            FROM_NUMBER NVARCHAR(20) NULL,
                            STATUS NVARCHAR(20) NULL,
                            STATUS_DESC NVARCHAR(255) NULL,
                            ERROR_NAME NVARCHAR(50) NULL,
                            DT_ENVIO NVARCHAR(23) NULL,
                            DT_ENTREGA NVARCHAR(23) NULL,
                            SMS_COUNT INT NULL,
                            VALOR_INFOBIP DECIMAL(18, 6) NULL,
                            MOEDA NVARCHAR(10) NULL,
                            FINAL_STATUS NVARCHAR(20) NULL,
                            FINAL_STATUS_DESC NVARCHAR(255) NULL
                        );
                        """
                    )
                    cursor.fast_executemany = True
                    cursor.setinputsizes(
                        [
                            None,  # ID_REGISTRO
                            None,  # ID_MENSAGEM
                            None,  # TO_NUMBER
                            None,  # FROM_NUMBER
                            None,  # STATUS
                            None,  # STATUS_DESC
                            None,  # ERROR_NAME
                            None,  # DT_ENVIO
                            None,  # DT_ENTREGA
                            None,  # SMS_COUNT
                            (pyodbc.SQL_DECIMAL, 18, 6),  # VALOR_INFOBIP
                            None,  # MOEDA
                            None,  # FINAL_STATUS
                            None,  # FINAL_STATUS_DESC
                        ]
                    )
                    cursor.executemany(
                        """
                        INSERT INTO #tmp_status (
                            ID_REGISTRO,
                            ID_MENSAGEM,
                            TO_NUMBER,
                            FROM_NUMBER,
                            STATUS,
                            STATUS_DESC,
                            ERROR_NAME,
                            DT_ENVIO,
                            DT_ENTREGA,
                            SMS_COUNT,
                            VALOR_INFOBIP,
                            MOEDA,
                            FINAL_STATUS,
                            FINAL_STATUS_DESC
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        [
                            (
                                item.get("id_registro"),
                                _clip(item.get("id_mensagem"), 100),
                                _clip(item.get("to_number"), 20),
                                _clip(item.get("from_number"), 20),
                                _clip(item.get("status"), 20),
                                _clip(item.get("status_desc"), 255),
                                _clip(item.get("error_name"), 50),
                                item.get("dt_envio"),
                                item.get("dt_entrega"),
                                _to_int(item.get("sms_count")),
                                _to_decimal_str(item.get("valor_infobip")),
                                _clip(item.get("moeda"), 10),
                                _clip(item.get("final_status"), 20),
                                _clip(item.get("final_status_desc"), 255),
                            )
                            for item in chunk
                        ]
                    )

                    cursor.execute(
                        """
                        UPDATE m
                        SET
                            ID_MENSAGEM = COALESCE(t.ID_MENSAGEM, m.ID_MENSAGEM),
                            NUM_CLIENTE = COALESCE(t.TO_NUMBER, m.NUM_CLIENTE),
                            FROM_NUMBER = t.FROM_NUMBER,
                            STATUS = COALESCE(t.FINAL_STATUS, t.STATUS, m.STATUS),
                            STATUS_DESC = COALESCE(t.FINAL_STATUS_DESC, t.STATUS_DESC, m.STATUS_DESC),
                            ERROR_NAME_INFOBIP = COALESCE(t.ERROR_NAME, m.ERROR_NAME_INFOBIP),
                            DT_ENVIO = m.DT_ENVIO,
                            DT_ENTREGA = COALESCE(TRY_CONVERT(datetime, t.DT_ENTREGA, 121), m.DT_ENTREGA),
                            SMS_COUNT = t.SMS_COUNT,
                            VALOR_INFOBIP = t.VALOR_INFOBIP,
                            CUSTO_TOTAL = CASE
                                WHEN t.SMS_COUNT IS NOT NULL AND t.VALOR_INFOBIP IS NOT NULL
                                    THEN CAST(t.SMS_COUNT AS DECIMAL(18, 6)) * t.VALOR_INFOBIP
                                ELSE m.CUSTO_TOTAL
                            END,
                            MOEDA = COALESCE(t.MOEDA, m.MOEDA),
                            DT_ATUALIZACAO = GETDATE()
                        FROM infobip.TB_ENVIO_RCS_MENSAGEM m
                        INNER JOIN #tmp_status t ON t.ID_REGISTRO = m.ID;
                        """
                    )
                    rows_m = cursor.rowcount

                    if logger:
                        logger.info(
                            "Batch status update: itens=%s rows_tb=%s",
                            len(chunk),
                            rows_m
                        )
                        if rows_m == 0:
                            logger.warning("Batch status update: 0 linhas em TB_ENVIO_RCS_MENSAGEM")
                except Exception:
                    if logger:
                        logger.exception("Erro ao atualizar status em batch (itens=%s)", len(chunk))
                    raise
                finally:
                    try:
                        cursor.execute("DROP TABLE #tmp_status;")
                    except Exception:
                        pass
            conn.commit()


    @staticmethod
    def marcar_erro(id_registro, erro):
        if erro is not None:
            erro = str(erro)[:50]
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS = 'ERRO',
                STATUS_DESC = ?,
                DT_ATUALIZACAO = GETDATE()
            WHERE ID = ?
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, erro, id_registro)
            conn.commit()

   
    @staticmethod
    def bloquear_mensagem_fora_padrao(id_registro, status_desc=None):
        status_value = "MSG_FORA_PADRAO"
        status_desc = status_desc or status_value
        status_desc = status_desc[:50]
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                STATUS = ?,
                STATUS_DESC = ?
            WHERE ID = ?
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, status_value, status_desc, id_registro)
            conn.commit()

    @staticmethod
    def adquirir_lock_envio(lock_timeout_ms=0, logger=None):
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                DECLARE @result int;
                EXEC @result = sp_getapplock
                    @Resource = 'ENVIO_RCS_LOCK',
                    @LockMode = 'Exclusive',
                    @LockOwner = 'Session',
                    @LockTimeout = ?;
                SELECT @result AS RESULT;
                """,
                lock_timeout_ms
            )
            row = cursor.fetchone()
            result = row.RESULT if row else -999
            if result < 0:
                if logger:
                    logger.warning("Lock envio nao adquirido (result=%s)", result)
                try:
                    conn.close()
                except Exception:
                    pass
                return None
            if logger:
                logger.info("Lock envio adquirido")
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise

    @staticmethod
    def liberar_lock_envio(conn, logger=None):
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                EXEC sp_releaseapplock
                    @Resource = 'ENVIO_RCS_LOCK',
                    @LockOwner = 'Session';
                """
            )
            if logger:
                logger.info("Lock envio liberado")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def arquivar_envios_antigos(logger=None):
        query = "EXEC infobip.SP_ARQUIVAR_TB_ENVIO_RCS_MENSAGEM;"

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            if logger:
                logger.info("Arquivamento RCS executado")

    @staticmethod
    def contar_enviados_mes_total():
        query = """
            DECLARE @inicio DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1);
            DECLARE @fim DATE = DATEADD(MONTH, 1, @inicio);

            SELECT COUNT(*) AS TOTAL
            FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
            WHERE DT_ENVIO >= @inicio
              AND DT_ENVIO < @fim
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            return row.TOTAL if row else 0

    @staticmethod
    def contar_enviados_semana_total():
        query = """
            DECLARE @hoje DATE = CAST(GETDATE() AS DATE);
            DECLARE @inicio DATE = DATEADD(DAY, -(DATEDIFF(DAY, '19000101', @hoje) % 7), @hoje);
            DECLARE @fim DATE = DATEADD(DAY, 7, @inicio);

            SELECT COUNT(*) AS TOTAL
            FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
            WHERE DT_ENVIO >= @inicio
              AND DT_ENVIO < @fim
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            return row.TOTAL if row else 0

    @staticmethod
    def buscar_resumo_semanal_limite_cedente():
        query = f"""
            DECLARE @hoje DATE = CAST(GETDATE() AS DATE);
            DECLARE @inicio_semana DATE = DATEADD(DAY, -(DATEDIFF(DAY, '19000101', @hoje) % 7), @hoje);
            DECLARE @fim_semana DATE = DATEADD(DAY, 7, @inicio_semana);
            DECLARE @inicio_mes DATE = DATEFROMPARTS(YEAR(@hoje), MONTH(@hoje), 1);
            DECLARE @inicio_semana_mes DATE = DATEADD(DAY, -(DATEDIFF(DAY, '19000101', @inicio_mes) % 7), @inicio_mes);
            DECLARE @inicio_acumulo_mes DATE =
                CASE
                    WHEN @inicio_semana_mes < @inicio_mes THEN DATEADD(DAY, 7, @inicio_semana_mes)
                    ELSE @inicio_semana_mes
                END;
            DECLARE @semanas_anteriores INT =
                CASE
                    WHEN @inicio_semana <= @inicio_acumulo_mes THEN 0
                    ELSE DATEDIFF(DAY, @inicio_acumulo_mes, @inicio_semana) / 7
                END;

            WITH limites_base AS (
                SELECT
                    CEDENTE,
                    MAX(ID_CEDENTE) AS ID_CEDENTE,
                    MAX(LIMITE_MENSAL) AS LIMITE_SEMANAL
                FROM infobip.TB_CEDENTE_RCS_LIMITE_SEMANAL WITH (NOLOCK)
                WHERE ATIVO = 1
                  AND UPPER(LTRIM(RTRIM(CEDENTE))) <> 'TOTAL'
                GROUP BY CEDENTE
            ),
            limite_total AS (
                SELECT MAX(LIMITE_TOTAL) AS LIMITE_SEMANAL
                FROM {EnvioRepository.LIMITE_TOTAL_SEMANAL_TABLE} WITH (NOLOCK)
                WHERE ATIVO = 1
            ),
            enviados_semana_atual_cedente AS (
                SELECT
                    CEDENTE,
                    COUNT(*) AS TOTAL_ENVIADO
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_semana
                  AND DT_ENVIO < @fim_semana
                GROUP BY CEDENTE
            ),
            enviados_semana_atual_total AS (
                SELECT COUNT(*) AS TOTAL_ENVIADO
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_semana
                  AND DT_ENVIO < @fim_semana
            ),
            enviados_anteriores_mes_cedente AS (
                SELECT
                    CEDENTE,
                    COUNT(*) AS TOTAL_ENVIADO_ANTERIOR_MES
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_acumulo_mes
                  AND DT_ENVIO < @inicio_semana
                GROUP BY CEDENTE
            ),
            enviados_anteriores_mes_total AS (
                SELECT COUNT(*) AS TOTAL_ENVIADO_ANTERIOR_MES
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_acumulo_mes
                  AND DT_ENVIO < @inicio_semana
            )
            SELECT
                l.ID_CEDENTE,
                l.CEDENTE,
                l.LIMITE_SEMANAL,
                ISNULL(eac.TOTAL_ENVIADO, 0) AS TOTAL_ENVIADO,
                CASE
                    WHEN (@semanas_anteriores * l.LIMITE_SEMANAL) - ISNULL(emc.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                        THEN (@semanas_anteriores * l.LIMITE_SEMANAL) - ISNULL(emc.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                    ELSE 0
                END AS NAO_UTILIZADO,
                (
                    l.LIMITE_SEMANAL
                    + CASE
                        WHEN (@semanas_anteriores * l.LIMITE_SEMANAL) - ISNULL(emc.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                            THEN (@semanas_anteriores * l.LIMITE_SEMANAL) - ISNULL(emc.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                        ELSE 0
                      END
                ) AS LIMITE_EFETIVO,
                (
                    (
                        l.LIMITE_SEMANAL
                        + CASE
                            WHEN (@semanas_anteriores * l.LIMITE_SEMANAL) - ISNULL(emc.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                                THEN (@semanas_anteriores * l.LIMITE_SEMANAL) - ISNULL(emc.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                            ELSE 0
                          END
                    ) - ISNULL(eac.TOTAL_ENVIADO, 0)
                ) AS RESTANTE
            FROM limites_base l
            LEFT JOIN enviados_semana_atual_cedente eac ON eac.CEDENTE = l.CEDENTE
            LEFT JOIN enviados_anteriores_mes_cedente emc ON emc.CEDENTE = l.CEDENTE
            UNION ALL
            SELECT
                999999 AS ID_CEDENTE,
                'TOTAL' AS CEDENTE,
                lt.LIMITE_SEMANAL,
                ISNULL(eat.TOTAL_ENVIADO, 0) AS TOTAL_ENVIADO,
                CASE
                    WHEN (@semanas_anteriores * lt.LIMITE_SEMANAL) - ISNULL(emt.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                        THEN (@semanas_anteriores * lt.LIMITE_SEMANAL) - ISNULL(emt.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                    ELSE 0
                END AS NAO_UTILIZADO,
                (
                    lt.LIMITE_SEMANAL
                    + CASE
                        WHEN (@semanas_anteriores * lt.LIMITE_SEMANAL) - ISNULL(emt.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                            THEN (@semanas_anteriores * lt.LIMITE_SEMANAL) - ISNULL(emt.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                        ELSE 0
                    END
                ) AS LIMITE_EFETIVO,
                (
                    (
                        lt.LIMITE_SEMANAL
                        + CASE
                            WHEN (@semanas_anteriores * lt.LIMITE_SEMANAL) - ISNULL(emt.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                                THEN (@semanas_anteriores * lt.LIMITE_SEMANAL) - ISNULL(emt.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                            ELSE 0
                        END
                    ) - ISNULL(eat.TOTAL_ENVIADO, 0)
                ) AS RESTANTE
            FROM limite_total lt
            CROSS JOIN enviados_semana_atual_total eat
            CROSS JOIN enviados_anteriores_mes_total emt
            WHERE lt.LIMITE_SEMANAL IS NOT NULL
            ORDER BY CEDENTE;
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def buscar_resumo_relatorio_limite_cedente():
        query = f"""
            DECLARE @hoje DATE = CAST(GETDATE() AS DATE);
            DECLARE @inicio_semana DATE = DATEADD(DAY, -(DATEDIFF(DAY, '19000101', @hoje) % 7), @hoje);
            DECLARE @fim_semana DATE = DATEADD(DAY, 7, @inicio_semana);
            DECLARE @inicio_mes DATE = DATEFROMPARTS(YEAR(@hoje), MONTH(@hoje), 1);
            DECLARE @fim_mes DATE = DATEADD(MONTH, 1, @inicio_mes);
            DECLARE @inicio_semana_mes DATE = DATEADD(DAY, -(DATEDIFF(DAY, '19000101', @inicio_mes) % 7), @inicio_mes);
            DECLARE @inicio_acumulo_mes DATE =
                CASE
                    WHEN @inicio_semana_mes < @inicio_mes THEN DATEADD(DAY, 7, @inicio_semana_mes)
                    ELSE @inicio_semana_mes
                END;
            DECLARE @semanas_anteriores INT =
                CASE
                    WHEN @inicio_semana <= @inicio_acumulo_mes THEN 0
                    ELSE DATEDIFF(DAY, @inicio_acumulo_mes, @inicio_semana) / 7
                END;

            WITH limites_semana AS (
                SELECT
                    CEDENTE,
                    MAX(ID_CEDENTE) AS ID_CEDENTE,
                    MAX(LIMITE_MENSAL) AS LIMITE_SEMANAL
                FROM infobip.TB_CEDENTE_RCS_LIMITE_SEMANAL WITH (NOLOCK)
                WHERE ATIVO = 1
                  AND UPPER(LTRIM(RTRIM(CEDENTE))) <> 'TOTAL'
                GROUP BY CEDENTE
            ),
            limite_total_semana AS (
                SELECT MAX(LIMITE_TOTAL) AS LIMITE_SEMANAL
                FROM {EnvioRepository.LIMITE_TOTAL_SEMANAL_TABLE} WITH (NOLOCK)
                WHERE ATIVO = 1
            ),
            enviados_semana_atual_cedente AS (
                SELECT
                    CEDENTE,
                    COUNT(*) AS TOTAL_ENVIADO_SEMANA
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_semana
                  AND DT_ENVIO < @fim_semana
                GROUP BY CEDENTE
            ),
            enviados_semana_atual_total AS (
                SELECT COUNT(*) AS TOTAL_ENVIADO_SEMANA
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_semana
                  AND DT_ENVIO < @fim_semana
            ),
            enviados_anteriores_mes_cedente AS (
                SELECT
                    CEDENTE,
                    COUNT(*) AS TOTAL_ENVIADO_ANTERIOR_MES
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_acumulo_mes
                  AND DT_ENVIO < @inicio_semana
                GROUP BY CEDENTE
            ),
            enviados_anteriores_mes_total AS (
                SELECT COUNT(*) AS TOTAL_ENVIADO_ANTERIOR_MES
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_acumulo_mes
                  AND DT_ENVIO < @inicio_semana
            ),
            limites_mes AS (
                SELECT
                    CEDENTE,
                    MAX(LIMITE_MENSAL) AS LIMITE_MENSAL
                FROM infobip.TB_CEDENTE_RCS_LIMITE_MENSAL WITH (NOLOCK)
                WHERE ATIVO = 1
                  AND UPPER(LTRIM(RTRIM(CEDENTE))) <> 'TOTAL'
                GROUP BY CEDENTE
            ),
            limite_total_mes AS (
                SELECT MAX(LIMITE_TOTAL) AS LIMITE_MENSAL
                FROM {EnvioRepository.LIMITE_TOTAL_MENSAL_TABLE} WITH (NOLOCK)
                WHERE ATIVO = 1
            ),
            enviados_mes_cedente AS (
                SELECT
                    CEDENTE,
                    COUNT(*) AS TOTAL_ENVIADO_MES
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_mes
                  AND DT_ENVIO < @fim_mes
                GROUP BY CEDENTE
            ),
            enviados_mes_total AS (
                SELECT COUNT(*) AS TOTAL_ENVIADO_MES
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio_mes
                  AND DT_ENVIO < @fim_mes
            )
            SELECT
                ls.ID_CEDENTE,
                ls.CEDENTE,
                ls.LIMITE_SEMANAL,
                ISNULL(esc.TOTAL_ENVIADO_SEMANA, 0) AS TOTAL_ENVIADO,
                CASE
                    WHEN (@semanas_anteriores * ls.LIMITE_SEMANAL) - ISNULL(eamc.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                        THEN (@semanas_anteriores * ls.LIMITE_SEMANAL) - ISNULL(eamc.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                    ELSE 0
                END AS NAO_UTILIZADO,
                (
                    ls.LIMITE_SEMANAL
                    + CASE
                        WHEN (@semanas_anteriores * ls.LIMITE_SEMANAL) - ISNULL(eamc.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                            THEN (@semanas_anteriores * ls.LIMITE_SEMANAL) - ISNULL(eamc.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                        ELSE 0
                    END
                ) AS LIMITE_EFETIVO,
                (
                    (
                        ls.LIMITE_SEMANAL
                        + CASE
                            WHEN (@semanas_anteriores * ls.LIMITE_SEMANAL) - ISNULL(eamc.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                                THEN (@semanas_anteriores * ls.LIMITE_SEMANAL) - ISNULL(eamc.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                            ELSE 0
                        END
                    ) - ISNULL(esc.TOTAL_ENVIADO_SEMANA, 0)
                ) AS RESTANTE,
                ISNULL(lm.LIMITE_MENSAL, 0) AS LIMITE_MENSAL,
                ISNULL(emc.TOTAL_ENVIADO_MES, 0) AS TOTAL_ENVIADO_MES,
                ISNULL(lm.LIMITE_MENSAL, 0) - ISNULL(emc.TOTAL_ENVIADO_MES, 0) AS RESTANTE_MES
            FROM limites_semana ls
            LEFT JOIN enviados_semana_atual_cedente esc ON esc.CEDENTE = ls.CEDENTE
            LEFT JOIN enviados_anteriores_mes_cedente eamc ON eamc.CEDENTE = ls.CEDENTE
            LEFT JOIN limites_mes lm ON lm.CEDENTE = ls.CEDENTE
            LEFT JOIN enviados_mes_cedente emc ON emc.CEDENTE = ls.CEDENTE
            UNION ALL
            SELECT
                999999 AS ID_CEDENTE,
                'TOTAL' AS CEDENTE,
                lts.LIMITE_SEMANAL,
                ISNULL(est.TOTAL_ENVIADO_SEMANA, 0) AS TOTAL_ENVIADO,
                CASE
                    WHEN (@semanas_anteriores * lts.LIMITE_SEMANAL) - ISNULL(eamt.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                        THEN (@semanas_anteriores * lts.LIMITE_SEMANAL) - ISNULL(eamt.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                    ELSE 0
                END AS NAO_UTILIZADO,
                (
                    lts.LIMITE_SEMANAL
                    + CASE
                        WHEN (@semanas_anteriores * lts.LIMITE_SEMANAL) - ISNULL(eamt.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                            THEN (@semanas_anteriores * lts.LIMITE_SEMANAL) - ISNULL(eamt.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                        ELSE 0
                    END
                ) AS LIMITE_EFETIVO,
                (
                    (
                        lts.LIMITE_SEMANAL
                        + CASE
                            WHEN (@semanas_anteriores * lts.LIMITE_SEMANAL) - ISNULL(eamt.TOTAL_ENVIADO_ANTERIOR_MES, 0) > 0
                                THEN (@semanas_anteriores * lts.LIMITE_SEMANAL) - ISNULL(eamt.TOTAL_ENVIADO_ANTERIOR_MES, 0)
                            ELSE 0
                        END
                    ) - ISNULL(est.TOTAL_ENVIADO_SEMANA, 0)
                ) AS RESTANTE,
                ISNULL(ltm.LIMITE_MENSAL, 0) AS LIMITE_MENSAL,
                ISNULL(emt.TOTAL_ENVIADO_MES, 0) AS TOTAL_ENVIADO_MES,
                ISNULL(ltm.LIMITE_MENSAL, 0) - ISNULL(emt.TOTAL_ENVIADO_MES, 0) AS RESTANTE_MES
            FROM limite_total_semana lts
            CROSS JOIN limite_total_mes ltm
            CROSS JOIN enviados_semana_atual_total est
            CROSS JOIN enviados_anteriores_mes_total eamt
            CROSS JOIN enviados_mes_total emt
            WHERE lts.LIMITE_SEMANAL IS NOT NULL OR ltm.LIMITE_MENSAL IS NOT NULL
            ORDER BY CEDENTE;
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def buscar_resumo_mensal_limite_cedente():
        query = f"""
            DECLARE @inicio DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1);
            DECLARE @fim DATE = DATEADD(MONTH, 1, @inicio);

            WITH limites AS (
                SELECT
                    CEDENTE,
                    MAX(ID_CEDENTE) AS ID_CEDENTE,
                    MAX(LIMITE_MENSAL) AS LIMITE_MENSAL
                FROM infobip.TB_CEDENTE_RCS_LIMITE_MENSAL WITH (NOLOCK)
                WHERE ATIVO = 1
                  AND UPPER(LTRIM(RTRIM(CEDENTE))) <> 'TOTAL'
                GROUP BY CEDENTE
            ),
            limite_total AS (
                SELECT MAX(LIMITE_TOTAL) AS LIMITE_MENSAL
                FROM {EnvioRepository.LIMITE_TOTAL_MENSAL_TABLE} WITH (NOLOCK)
                WHERE ATIVO = 1
            ),
            enviados_cedente AS (
                SELECT
                    CEDENTE,
                    COUNT(*) AS TOTAL_ENVIADO
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio
                  AND DT_ENVIO < @fim
                GROUP BY CEDENTE
            ),
            enviados_total AS (
                SELECT COUNT(*) AS TOTAL_ENVIADO
                FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
                WHERE DT_ENVIO >= @inicio
                  AND DT_ENVIO < @fim
            )
            SELECT
                l.ID_CEDENTE,
                l.CEDENTE,
                l.LIMITE_MENSAL,
                ISNULL(ec.TOTAL_ENVIADO, 0) AS TOTAL_ENVIADO,
                l.LIMITE_MENSAL - ISNULL(ec.TOTAL_ENVIADO, 0) AS RESTANTE
            FROM limites l
            LEFT JOIN enviados_cedente ec ON ec.CEDENTE = l.CEDENTE
            UNION ALL
            SELECT
                999999 AS ID_CEDENTE,
                'TOTAL' AS CEDENTE,
                lt.LIMITE_MENSAL,
                ISNULL(et.TOTAL_ENVIADO, 0) AS TOTAL_ENVIADO,
                lt.LIMITE_MENSAL - ISNULL(et.TOTAL_ENVIADO, 0) AS RESTANTE
            FROM limite_total lt
            CROSS JOIN enviados_total et
            WHERE lt.LIMITE_MENSAL IS NOT NULL
            ORDER BY CEDENTE;
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def contar_enviados_periodo(inicio, fim):
        query = """
            SELECT COUNT(*) AS TOTAL
            FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
            WHERE DT_ENVIO >= ? AND DT_ENVIO < ?
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, inicio, fim)
            row = cursor.fetchone()
            return row.TOTAL if row else 0

    @staticmethod
    def contar_enviados_por_cedente_periodo(inicio, fim):
        query = """
            SELECT CEDENTE, COUNT(*) AS TOTAL
            FROM infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA WITH (NOLOCK)
            WHERE DT_ENVIO >= ? AND DT_ENVIO < ?
            GROUP BY CEDENTE
            ORDER BY CEDENTE
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, inicio, fim)
            return cursor.fetchall()

    @staticmethod
    def existe_alerta_hora(ref_inicio):
        query = """
            SELECT TOP (1) ALERTA_HORA_ENVIADO_EM
            FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
            WHERE ALERTA_HORA_REF = ?
              AND ALERTA_HORA_ENVIADO_EM IS NOT NULL
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, ref_inicio)
            row = cursor.fetchone()
            return row is not None

    @staticmethod
    def registrar_alerta_hora(ref_inicio, ref_fim, qtd):
        query = """
            UPDATE infobip.TB_ENVIO_RCS_MENSAGEM
            SET
                ALERTA_HORA_REF = ?,
                ALERTA_HORA_ENVIADO_EM = GETDATE(),
                ALERTA_HORA_QTD = ?
            WHERE ID = (
                SELECT TOP (1) ID
                FROM infobip.TB_ENVIO_RCS_MENSAGEM WITH (NOLOCK)
                WHERE DT_ENVIO >= ? AND DT_ENVIO < ?
                ORDER BY DT_ENVIO DESC
            )
        """

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, ref_inicio, qtd, ref_inicio, ref_fim)
            conn.commit()

