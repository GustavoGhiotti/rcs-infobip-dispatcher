import argparse
import traceback
from src.application.services.envio_service import EnvioService
from src.application.services.status_service import StatusService
from src.infrastructure.logging.logger import get_logger
from src.repository.envio_repository import EnvioRepository

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "acao",
        choices=["enviar", "status", "relatorio", "arquivar"])
    args = parser.parse_args()

    try:
        if args.acao == "enviar":
            service = EnvioService()
            service.processar_envios()
        elif args.acao == "status":
            service = StatusService()
            service.processar_status()
        elif args.acao == "relatorio":
            service = EnvioService()
            service.enviar_relatorio_hora_manual()
        elif args.acao == "arquivar":
            logger = get_logger("arquivamento")
            EnvioRepository.arquivar_envios_antigos(logger=logger)
    except Exception as exc:
        print(f"Erro fatal ao executar '{args.acao}': {exc}")
        traceback.print_exc()
        raise
