import smtplib
from email.message import EmailMessage
from datetime import datetime

from src.infrastructure.logging.logger import get_logger
from src.shared.utils.env_load import dados_email


class Email:
    destinatarios = [
        "colaborador19@empresa.example",
        "colaborador18@empresa.example",
        "desenvolvimento@empresa.example",
        "planejamento@empresa.example"
    ]

    @staticmethod
    def enviar_alerta_hora(
        qtd_enviadas: int,
        inicio: datetime,
        fim: datetime,
        por_cedente,
        total_dia: int,
        total_semana: int,
        total_mes: int,
        resumo_semanal,
        permitir_qtd_zero: bool = False
    ) -> None:
        logger = get_logger("email")
        if qtd_enviadas == 0 and not permitir_qtd_zero:
            return
        email_config = dados_email()

        remetente = email_config["email"]
        senha = email_config["senha"]
        destinatarios = Email.destinatarios

        msg = EmailMessage()
        msg["Subject"] = "Alerta RCS - Enviados na ultima hora"
        msg["From"] = remetente
        msg["To"] = ", ".join(destinatarios)

        linhas_cedente = []
        for row in por_cedente:
            linhas_cedente.append(f"- {row.CEDENTE}: {row.TOTAL}")
        detalhe = "\n".join(linhas_cedente) if linhas_cedente else "Sem envios por cedente."

        linhas_semana = []
        for row in resumo_semanal:
            if str(getattr(row, "CEDENTE", "") or "").strip().upper() == "TOTAL":
                continue
            nao_utilizado = int(getattr(row, "NAO_UTILIZADO", 0) or 0)
            total_enviado = int(getattr(row, "TOTAL_ENVIADO", 0) or 0)
            limite_semanal = int(getattr(row, "LIMITE_SEMANAL", 0) or 0)
            limite_total_semana = limite_semanal + nao_utilizado
            restante_total_semana = max(0, limite_total_semana - total_enviado)
            restante_mes = int(getattr(row, "RESTANTE_MES", 0) or 0)
            linhas_semana.append(
                f"- {row.ID_CEDENTE} | {row.CEDENTE}: "
                f"{total_enviado}/{limite_total_semana} "
                f"(limite_semanal: {limite_semanal}) + "
                f"(nao utilizado: {nao_utilizado}) = "
                f"(restante: {restante_total_semana} | restante_mes: {restante_mes})"
            )
        detalhe_semana = "\n".join(linhas_semana) if linhas_semana else "Sem limites semanais ativos."

        conteudo = (
            "Resumo de envio de RCS.\n"
            f"Periodo: {inicio:%d/%m/%Y %H:%M} - {fim:%d/%m/%Y %H:%M}\n"
            f"Quantidade total enviadas hora: {qtd_enviadas}\n"
            f"Total enviado no dia: {total_dia}\n"
            f"Total enviado na semana: {total_semana}\n"
            f"Total enviado no mes: {total_mes}\n"
            "Envios por cedente:\n"
            f"{detalhe}\n"
            "Resumo semanal por cedente (total/limite e restante):\n"
            f"{detalhe_semana}\n"
        )

        msg.set_content(conteudo)

        try:
            if email_config["use_ssl"]:
                with smtplib.SMTP_SSL(email_config["host"], email_config["port"]) as smtp:
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(email_config["host"], email_config["port"]) as smtp:
                    smtp.starttls()
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            logger.info(
                "Email enviado: alerta hora (%s - %s) total=%s",
                inicio.strftime("%d/%m/%Y %H:%M"),
                fim.strftime("%d/%m/%Y %H:%M"),
                qtd_enviadas
            )
        except Exception as e:
            logger.exception("Erro ao enviar e-mail de alerta: %s", str(e))

    @staticmethod
    def enviar_alerta_cedente(cedente: str, qtd_envios: int, inicio: datetime, fim: datetime) -> None:
        logger = get_logger("email")
        email_config = dados_email()

        remetente = email_config["email"]
        senha = email_config["senha"]
        destinatarios = Email.destinatarios

        msg = EmailMessage()
        msg["Subject"] = "Alerta RCS - Cedente acima do limite por hora"
        msg["From"] = remetente
        msg["To"] = ", ".join(destinatarios)

        conteudo = (
            "Alerta de quantidade por cedente.\n"
            f"Cedente: {cedente}\n"
            f"Periodo: {inicio:%d/%m/%Y %H:%M} - {fim:%d/%m/%Y %H:%M}\n"
            f"Quantidade enviada na ultima hora: {qtd_envios}\n"
        )

        msg.set_content(conteudo)

        try:
            if email_config["use_ssl"]:
                with smtplib.SMTP_SSL(email_config["host"], email_config["port"]) as smtp:
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(email_config["host"], email_config["port"]) as smtp:
                    smtp.starttls()
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            logger.info(
                "Email enviado: alerta cedente (%s) total_ultima_hora=%s",
                cedente,
                qtd_envios
            )
        except Exception as e:
            logger.exception("Erro ao enviar e-mail de alerta cedente: %s", str(e))

    @staticmethod
    def enviar_alerta_pendentes_cedente(cedente: str, qtd_pendentes: int, data_ref: datetime) -> None:
        logger = get_logger("email")
        email_config = dados_email()

        remetente = email_config["email"]
        senha = email_config["senha"]
        destinatarios = Email.destinatarios

        msg = EmailMessage()
        msg["Subject"] = "Alerta RCS - Bloqueio por quantidade"
        msg["From"] = remetente
        msg["To"] = ", ".join(destinatarios)

        conteudo = (
            "Envio bloqueado por quantidade de casos.\n"
            f"Cedente: {cedente}\n"
            f"Data/hora: {data_ref:%d/%m/%Y %H:%M}\n"
            f"Quantidade pendente: {qtd_pendentes}\n"
        )

        msg.set_content(conteudo)

        try:
            if email_config["use_ssl"]:
                with smtplib.SMTP_SSL(email_config["host"], email_config["port"]) as smtp:
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(email_config["host"], email_config["port"]) as smtp:
                    smtp.starttls()
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            logger.info(
                "Email enviado: bloqueio pendentes (%s) total_pendentes=%s",
                cedente,
                qtd_pendentes
            )
        except Exception as e:
            logger.exception("Erro ao enviar e-mail de alerta pendentes: %s", str(e))

    @staticmethod
    def enviar_alerta_limite_semanal(data_ref: datetime, por_cedente) -> None:
        logger = get_logger("email")
        email_config = dados_email()

        remetente = email_config["email"]
        senha = email_config["senha"]
        destinatarios = Email.destinatarios

        msg = EmailMessage()
        msg["Subject"] = "Alerta RCS - Limite semanal atingido"
        msg["From"] = remetente
        msg["To"] = ", ".join(destinatarios)

        linhas_cedente = []
        for row in por_cedente:
            linhas_cedente.append(f"- {row['id_cedente']} | {row['cedente']} - envios acima do limite semanal {row['total']}")
        detalhe = "\n".join(linhas_cedente) if linhas_cedente else "Sem bloqueios por cedente."

        conteudo = (
            "Envios bloqueados por limite semanal.\n"
            f"Data/hora: {data_ref:%d/%m/%Y %H:%M}\n"
            "Bloqueios por cedente:\n"
            f"{detalhe}\n"
        )

        msg.set_content(conteudo)

        try:
            if email_config["use_ssl"]:
                with smtplib.SMTP_SSL(email_config["host"], email_config["port"]) as smtp:
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(email_config["host"], email_config["port"]) as smtp:
                    smtp.starttls()
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            logger.info("Email enviado: limite semanal (bloqueios=%s)", len(por_cedente))
        except Exception as e:
            logger.exception("Erro ao enviar e-mail de limite semanal: %s", str(e))

    @staticmethod
    def enviar_alerta_limite_mensal(data_ref: datetime, por_cedente) -> None:
        logger = get_logger("email")
        email_config = dados_email()

        remetente = email_config["email"]
        senha = email_config["senha"]
        destinatarios = Email.destinatarios

        msg = EmailMessage()
        msg["Subject"] = "Alerta RCS - Limite mensal atingido"
        msg["From"] = remetente
        msg["To"] = ", ".join(destinatarios)

        linhas_cedente = []
        for row in por_cedente:
            linhas_cedente.append(f"- {row['id_cedente']} | {row['cedente']} - envios acima do limite mensal {row['total']}")
        detalhe = "\n".join(linhas_cedente) if linhas_cedente else "Sem bloqueios por cedente."

        conteudo = (
            "Envios bloqueados por limite mensal.\n"
            f"Data/hora: {data_ref:%d/%m/%Y %H:%M}\n"
            "Bloqueios por cedente:\n"
            f"{detalhe}\n"
        )

        msg.set_content(conteudo)

        try:
            if email_config["use_ssl"]:
                with smtplib.SMTP_SSL(email_config["host"], email_config["port"]) as smtp:
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(email_config["host"], email_config["port"]) as smtp:
                    smtp.starttls()
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            logger.info("Email enviado: limite mensal (bloqueios=%s)", len(por_cedente))
        except Exception as e:
            logger.exception("Erro ao enviar e-mail de limite mensal: %s", str(e))

    @staticmethod
    def enviar_alerta_cedente_fora_padrao(data_ref: datetime, por_cedente) -> None:
        logger = get_logger("email")
        email_config = dados_email()

        remetente = email_config["email"]
        senha = email_config["senha"]
        destinatarios = Email.destinatarios

        msg = EmailMessage()
        msg["Subject"] = "Alerta RCS - Cedente fora do padrao"
        msg["From"] = remetente
        msg["To"] = ", ".join(destinatarios)

        linhas_cedente = []
        for row in por_cedente:
            linhas_cedente.append(f"- {row['id_cedente']} | {row['cedente']} - Quantidade de envios: {row['total']}")
        detalhe = "\n".join(linhas_cedente) if linhas_cedente else "Sem bloqueios por cedente."

        conteudo = (
            "Envios bloqueados por cedente fora do padrao de insert.\n"
            f"Data/hora: {data_ref:%d/%m/%Y %H:%M}\n"
            "Bloqueios por cedente:\n"
            f"{detalhe}\n"
        )

        msg.set_content(conteudo)

        try:
            if email_config["use_ssl"]:
                with smtplib.SMTP_SSL(email_config["host"], email_config["port"]) as smtp:
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(email_config["host"], email_config["port"]) as smtp:
                    smtp.starttls()
                    smtp.login(remetente, senha)
                    smtp.send_message(msg)
            logger.info("Email enviado: cedente fora do padrao (bloqueios=%s)", len(por_cedente))
        except Exception as e:
            logger.exception("Erro ao enviar e-mail de cedente fora do padrao: %s", str(e))
