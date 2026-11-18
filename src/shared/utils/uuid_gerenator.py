from datetime import datetime
from uuid import uuid4


def _slug(value: str, max_len: int = 12) -> str:
    if not value:
        return "SEM"
    keep = []
    for ch in value.upper():
        if ch.isalnum():
            keep.append(ch)
        elif ch in (" ", "_", "-", "."):
            keep.append("-")
    compact = "".join(keep)
    while "--" in compact:
        compact = compact.replace("--", "-")
    compact = compact.strip("-")
    if not compact:
        return "SEM"
    return compact[:max_len]


def build_custom_message_id(
    cedente: str,
    nm_usuario_envio: str,
    id_registro=None
) -> str:
    return ensure_unique_custom_message_id("RCS", id_registro)


def ensure_unique_custom_message_id(value: str, id_registro=None) -> str:
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = uuid4().hex[:6].upper()
    if id_registro is None:
        return f"RCS-{timestamp}-{random_suffix}"
    return f"RCS-{id_registro}-{timestamp}-{random_suffix}"
