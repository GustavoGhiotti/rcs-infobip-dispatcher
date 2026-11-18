import json
from string import Formatter


def _extract_fields(template: str):
    fields = []
    for _, field_name, _, _ in Formatter().parse(template):
        if field_name:
            fields.append(field_name)
    return fields


def render_template(template: str, variables_json: str | None) -> str:
    if not template:
        return ""
    if not variables_json:
        required = _extract_fields(template)
        if required:
            faltando = ", ".join(required)
            raise ValueError(f"Variaveis ausentes: {faltando}")
        return template

    try:
        variables = json.loads(variables_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Variaveis JSON invalido: {exc}") from exc

    if not isinstance(variables, dict):
        raise ValueError("Variaveis JSON deve ser um objeto")

    required = _extract_fields(template)
    missing = []
    for name in required:
        if name not in variables:
            missing.append(name)
            continue
        value = variables.get(name)
        if value is None:
            missing.append(name)
            continue
        if isinstance(value, str) and value.strip() == "":
            missing.append(name)
    if missing:
        faltando = ", ".join(missing)
        raise ValueError(f"Variaveis ausentes: {faltando}")

    return template.format(**variables)
