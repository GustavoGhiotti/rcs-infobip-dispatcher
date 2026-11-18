import os
import threading
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from src.infrastructure.infobip.payloads import build_rcs_payload, build_rcs_payload_batch
from src.infrastructure.logging.logger import get_logger

load_dotenv()


class InfobipClient:
    def __init__(self, logger=None):
        self.logger = logger or get_logger(self.__class__.__name__)
        self.base_url = os.getenv("INFOBIP_BASE_URL")
        self.api_key = os.getenv("INFOBIP_API_KEY")
        self.sender = os.getenv("INFOBIP_RCS_FROM")

        if not self.base_url or not self.base_url.startswith(("http://", "https://")):
            if self.base_url:
                self.base_url = f"https://{self.base_url}"

        if not all([self.base_url, self.api_key, self.sender]):
            raise Exception("Configuração Infobip RCS incompleta no .env")

        self.headers = {
            "Authorization": f"App {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            respect_retry_after_header=True,
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self._status_rate_lock = threading.Lock()
        self._status_request_times = []

    def _throttle_status_requests(self):
        while True:
            with self._status_rate_lock:
                now = time.monotonic()
                self._status_request_times = [
                    ts for ts in self._status_request_times
                    if now - ts < 1.0
                ]
                if len(self._status_request_times) < 2:
                    self._status_request_times.append(now)
                    self.logger.info(
                        "Throttle status liberado: janela_1s=%s",
                        len(self._status_request_times)
                    )
                    return
                wait_time = max(0.0, 1.0 - (now - self._status_request_times[0]))
                self.logger.info(
                    "Throttle status aguardando: janela_1s=%s wait=%.3fs",
                    len(self._status_request_times),
                    wait_time
                )
            if wait_time > 0:
                time.sleep(wait_time)

    def send_rcs(self, to: str, content, custom_message_id: str):
        url = f"{self.base_url}/rcs/2/messages"

        payload = build_rcs_payload(
            to=to,
            content=content,
            custom_message_id=custom_message_id,
            sender=self.sender
        )

        self.logger.info(
            "POST %s CUSTOM_MESSAGE_ID=%s TO=%s",
            url,
            custom_message_id,
            to
        )
        response = self.session.post(
            url,
            json=payload,
            headers=self.headers,
            timeout=30
        )

        return {
            "http_status": response.status_code,
            "response_json": response.json() if response.content else None,
            "raw_response": response.text
        }

    def send_rcs_batch(self, messages):
        url = f"{self.base_url}/rcs/2/messages"

        payload = build_rcs_payload_batch(
            messages=messages,
            sender=self.sender
        )

        self.logger.info(
            "POST %s BATCH_QTD=%s SENDER=%s CONTENT_TYPE=%s",
            url,
            len(messages),
            self.sender,
            ",".join(sorted({str(item.get("content", {}).get("type", "SEM_TIPO")) for item in messages}))
        )
        response = self.session.post(
            url,
            json=payload,
            headers=self.headers,
            timeout=30
        )

        return {
            "http_status": response.status_code,
            "response_json": response.json() if response.content else None,
            "raw_response": response.text
        }

    def get_rcs_reports(self, message_id: str, limit: int = 1):
        url = f"{self.base_url}/rcs/2/reports"
        params = {
            "messageId": message_id,
            "limit": limit
        }

        self.logger.debug(
            "GET %s INFOBIP_ID=%s",
            url,
            message_id
        )
        self._throttle_status_requests()
        response = self.session.get(
            url,
            params=params,
            headers=self.headers,
            timeout=30
        )

        return {
            "http_status": response.status_code,
            "response_json": response.json() if response.content else None,
            "raw_response": response.text
        }

    def get_rcs_logs(self, message_id: str, limit: int = 1):
        url = f"{self.base_url}/rcs/2/logs"
        params = {
            "messageId": message_id,
            "limit": limit
        }

        self.logger.debug(
            "GET %s INFOBIP_ID=%s",
            url,
            message_id
        )
        self._throttle_status_requests()
        response = self.session.get(
            url,
            params=params,
            headers=self.headers,
            timeout=30
        )

        return {
            "http_status": response.status_code,
            "response_json": response.json() if response.content else None,
            "raw_response": response.text
        }

    def get_rcs_logs_batch(self, limit: int = 10000):
        url = f"{self.base_url}/rcs/2/logs"
        params = {
            "limit": limit
        }

        self.logger.debug(
            "GET %s LOGS_BATCH_LIMIT=%s",
            url,
            limit
        )
        self._throttle_status_requests()
        response = self.session.get(
            url,
            params=params,
            headers=self.headers,
            timeout=30
        )

        return {
            "http_status": response.status_code,
            "response_json": response.json() if response.content else None,
            "raw_response": response.text
        }

    def get_rcs_reports_bulk(self, bulk_id: str, limit: int = 10000):
        url = f"{self.base_url}/rcs/2/reports"
        params = {
            "bulkId": bulk_id,
            "limit": limit
        }

        self.logger.debug(
            "GET %s BULK_ID=%s",
            url,
            bulk_id
        )
        self._throttle_status_requests()
        response = self.session.get(
            url,
            params=params,
            headers=self.headers,
            timeout=30
        )

        return {
            "http_status": response.status_code,
            "response_json": response.json() if response.content else None,
            "raw_response": response.text
        }
