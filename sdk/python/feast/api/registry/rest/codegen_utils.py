import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


def render_template(template_name, context):
    template_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)), trim_blocks=True, lstrip_blocks=True
    )
    template = env.get_template(template_name)
    try:
        return template.render(**context)
    except Exception as e:
        logging.warning(
            f"Failed to render template {template_name} for {context.get('name', 'unknown')}: {e}"
        )
        return ""


def render_feature_view_code(context):
    return render_template("feature_view_template.jinja2", context)


def render_entity_code(context):
    return render_template("entity_template.jinja2", context)


def render_data_source_code(context):
    return render_template("data_source_template.jinja2", context)


def render_feature_service_code(context):
    return render_template("feature_service_template.jinja2", context)


def render_feature_code(context):
    return render_template("feature_template.jinja2", context)


def render_saved_dataset_code(context):
    return render_template("saved_dataset_template.jinja2", context)


def render_request_source_code(context):
    return render_template("request_source_template.jinja2", context)


def render_push_source_code(context):
    return render_template("push_source_template.jinja2", context)
