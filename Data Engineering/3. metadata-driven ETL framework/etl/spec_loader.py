import yaml
from jinja2 import Template

def load_spec(path: str, context: dict) -> dict:
    with open(path, "r") as f:
        raw = f.read()
    rendered = Template(raw).render(**context)
    return yaml.safe_load(rendered)