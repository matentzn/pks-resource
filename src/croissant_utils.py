from pathlib import Path
from typing import List, Dict, Union
from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.linkml_model.meta import SlotDefinition
from pathlib import Path
from datetime import date
from jinja2 import Environment, FileSystemLoader, StrictUndefined
import json

from mlcroissant import Dataset

def validate_croissant_metadata(croissant_metadata: Union[str, Dict]) -> Dataset:
    """
    Validate the given Croissant metadata, which can be a file path or a dict.
    Returns a Dataset object if valid, raises an exception if invalid.
    """
    if isinstance(croissant_metadata, str):
        dataset = Dataset(croissant_metadata)
    elif isinstance(croissant_metadata, dict):
        dataset = Dataset(croissant_metadata)
    else:
        raise ValueError("croissant_metadata must be a file path or a dict")
    
    # The Dataset constructor performs validation; if invalid, it raises an exception.
    return dataset

def write_croissant_metadata_to_file(matrix_kg_croissant_json: Dict, matrix_kg_croissant_file: str):
    """Write the Croissant metadata dict to a JSON file."""
    with open(matrix_kg_croissant_file, "w") as f:
        f.write(json.dumps(matrix_kg_croissant_json, indent=2))

def render_matrix_kg_template(matrix_schema, template_path: str) -> Dict:
    path = Path(template_path)
    env = Environment(
        loader=FileSystemLoader(str(path.parent)),
        autoescape=False,           # we’re generating JSON, not HTML
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined   # fail fast if a var is missing
    )
    template = env.get_template(path.name)

    node_columns = _extract_columns(matrix_schema, "UnionedNode")
    edge_columns = _extract_columns(matrix_schema, "UnionedEdge")

    context = {
        "date_modified": date.today().strftime("%Y-%m-%d"),
        "date_published": date.today().strftime("%Y-%m-%d"),
        "nodes_columns": node_columns,
        "edges_columns": edge_columns,
        "nodes_sha256": "REPLACE_ME_WITH_ACTUAL_SHA256",
        "edges_sha256": "REPLACE_ME_WITH_ACTUAL_SHA256",
    }

    # NOTE: expose variables at the top level (no nested `schema=` wrapper)
    rendered = template.render(**context)
    rendered_json = json.loads(rendered)
    return rendered_json


def _linkml_range_to_datatype(rng: str, sv: SchemaView) -> str:
    """
    Map a LinkML range to a simple scalar type expected by your template.
    Adjust as needed for your Croissant profile.
    """
    # Built-in LinkML scalars
    builtin = {
        "string": "Text",
        "integer": "Integer",
        "float": "Number",
        "double": "Number",
        "decimal": "Number",
        "boolean": "Boolean",
        "time": "Time",
        "date": "Date",
        "datetime": "DateTime",
        "uri": "Text",
        "uriorcurie": "Text",
        "ncname": "Text",
        "objectidentifier": "Text",
    }
    if rng in builtin:
        return builtin[rng]

    # If range is a type with a base
    t = sv.get_type(rng)
    if t and t.base:
        return builtin.get(t.base, "Text")

    # If range is an enum
    if sv.get_enum(rng):
        return "Text"

    # If range is another class (often referenced by CURIE in KGs)
    if sv.get_class(rng):
        return "Text"

    # Fallback
    return "Text"


def _is_nullable(slot: SlotDefinition) -> bool:
    """Nullable if not required and no positive min cardinality."""
    if getattr(slot, "required", False):
        return False
    # linkml_model uses min_cardinality (sometimes minimum_cardinality appears via conversions)
    min_card = getattr(slot, "min_cardinality", None) or getattr(slot, "minimum_cardinality", None)
    return not (min_card and int(min_card) > 0)


def _extract_columns(schemaview: SchemaView, class_name: str) -> List[Dict]:
    """
    Return a list of dicts like:
      { "name": <slot_name>, "dataType": <string>, "nullable": <bool> }
    suitable for your Jinja loop over edge_columns.
    """
    induced = schemaview.class_induced_slots(class_name)

    cols = []
    for s in induced:
        rng = s.range or "string"
        dt = _linkml_range_to_datatype(rng, schemaview)
        nullable = _is_nullable(s)

        cols.append(
            {
                "name": s.name,
                "dataType": dt,
                "nullable": bool(nullable),
            }
        )
    return cols
