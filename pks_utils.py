"""
Utilities for Primary Knowledge Sources (PKS) data processing.

This module provides helper functions for processing and analyzing
primary knowledge sources data from various registries.
"""

import yaml
import json
import pandas as pd
from jinja2 import Template
from typing import Dict, Any

##### Utilities for reading and writing

def save_yaml_file(data: Dict, file_path: str) -> None:
    """Save data to YAML file."""
    with open(file_path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)


def save_markdown_file(content: str, file_path: str) -> None:
    """Save content to markdown file."""
    with open(file_path, 'w') as f:
        f.write(content)

def load_yaml_file(file_path: str) -> Dict[str, Any]:
    """Load YAML file and return parsed data."""
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file and return parsed data."""
    with open(file_path, 'r') as f:
        return json.load(f)

##### Utilities for parsing the PKS source files

def _parse_source(source_data, source_id, id_column, extracted_metadata,ignored_metadata,primary_knowledge_sources):
    for record in source_data:
        raw_id = record[id_column]
        id = raw_id.replace("infores:", "")
        if id not in primary_knowledge_sources:
            
            primary_knowledge_sources[id] = {}

        for key in record:
            if key not in extracted_metadata + ignored_metadata + [id_column]:
                print(f"WARNING: Some potentially useful information from {source_id} is not included in the report: {key}")

        data_extract = {}
        data_extract[id_column] = raw_id
        for element in extracted_metadata:
            if element in record:
                data_extract[element] = record[element]

        primary_knowledge_sources[id][source_id] = data_extract

def _apply_infores_mapping(mapping, data_to_map, id_column):
    if mapping:
        for record in data_to_map:
            record['updated_id'] = mapping.get(record[id_column], record[id_column])

def parse_infores(infores_d, primary_knowledge_sources):
    source = 'infores'
    id_column = 'id'
    ignored_metadata = []
    extracted_metadata = ['id','status', 'name', 'description', 'knowledge_level', 'agent_type', 'url', 'xref', 'synonym', 'consumed_by', 'consumes']
    _parse_source(infores_d['information_resources'], source, id_column, extracted_metadata, ignored_metadata, primary_knowledge_sources)

def parse_reusabledata(reusabledata_d, primary_knowledge_sources, infores_mapping):
    source = 'reusabledata'
    id_column = 'updated_id'
    ignored_metadata = ['last-curated', 'grants']
    extracted_metadata = ['id', 'description', 'source', 'data-tags', 'grade-automatic', 'source-link', 'source-type', 'status', 'data-field', 'data-type', 'data-categories', 'data-access', 'license', 'license-type', 'license-link', 'license-hat-used', 'license-issues', 'license-commentary', 'license-commentary-embeddable', 'was-controversial', 'provisional', 'contacts']
    _apply_infores_mapping(infores_mapping, reusabledata_d, id_column='id')
    _parse_source(reusabledata_d, source, id_column, extracted_metadata, ignored_metadata, primary_knowledge_sources)

def parse_kgregistry(kgregistry_d, primary_knowledge_sources, infores_mapping):
    source = 'kgregistry'
    id_column = 'updated_id'
    ignored_metadata = ['products']
    extracted_metadata = ['id', 'activity_status', 'category', 'collection', 'contacts', 'creation_date', 'curators', 'description', 'domains', 'evaluation_page', 'fairsharing_id', 'homepage_url', 'infores_id', 'language', 'last_modified_date', 'layout', 'license', 'name', 'publications', 'repository', 'tags', 'usages', 'version', 'warnings']
    kgregistry_data = kgregistry_d['resources']
    _apply_infores_mapping(infores_mapping, kgregistry_data, 'id')
    
    # Since we already have a few infores ids in the kgregistry, we should use them, even if we dont have an explicit mapping
    for record in kgregistry_data:
        record['updated_id'] = record.get('infores_id', record['id'])
    _parse_source(kgregistry_data, source, id_column, extracted_metadata, ignored_metadata, primary_knowledge_sources)

def parse_matrixcurated(matrixcurated_d, primary_knowledge_sources):
    # Parse the manually curated pks specific information (mostly licensing information)
    source = 'matrixcurated'
    id_column = 'primary_knowledge_source'
    ignored_metadata = ['aggregator_knowledge_source', 'number_of_edges', 'infores_name', 'xref']
    extracted_metadata = ['license_name', 'license_source_link']
    _parse_source(matrixcurated_d.to_dict(orient="records"), source, id_column, extracted_metadata, ignored_metadata, primary_knowledge_sources)

def parse_matrixreviews(matrixreviews_d, primary_knowledge_sources):
    # Parse the manually curated pks reviews according to the rubric
    source = 'matrixreviews'
    id_column = 'primary_knowledge_source'
    ignored_metadata = ['infores_name']
    extracted_metadata = ['domain_coverage_score', 'domain_coverage_comments', 'source_scope_score', 'source_scope_score_comment', 'utility_drugrepurposing_score', 'utility_drugrepurposing_comment', 'label_rubric', 'label_rubric_rationale', 'label_manual', 'label_manual_comment', 'reviewer']
    _parse_source(matrixreviews_d.to_dict(orient="records"), source, id_column, extracted_metadata, ignored_metadata, primary_knowledge_sources)

def create_pks_subset_relevant_to_matrix(primary_knowledge_sources, relevant_sources):
    subset = {}
    for source in relevant_sources:
        if source in primary_knowledge_sources:
            subset[source] = primary_knowledge_sources[source]
    return subset

##### Utilities for generating the documentation

def _get_property( source_info, property, default_value = "Unknown"):
    property_value = default_value
    if 'infores' in source_info and property in source_info['infores']:
        property_value = source_info['infores'][property]
    elif 'kgregistry' in source_info and property in source_info['kgregistry']:
        property_value = source_info['kgregistry'][property]
    elif 'reusabledata' in source_info and property in source_info['reusabledata']:
        property_value = source_info['reusabledata'][property]
    return property_value
    
def _format_license(source_info):
    license_info_matrix = "Not available"
    return license_info_matrix

def _format_review(source_info):
    review = "Not available"
    return review

def generate_list_of_pks_markdown_strings(source_data):
    pks_jinja2_template = Template("""### Source: {{ title }} ({{ id }})

_{{ description }}_

- **URL**: {{ url }}
- **License**: {{ license }}
- **Review**: {{ review }}""")

    pks_documentation_texts = []
    for source_id, source_info in source_data.items():
        name = _get_property(source_info, 'name', default_value="No name")
        description = _get_property(source_info, 'description', default_value="No description.")
        license = _format_license(source_info)
        review = _format_review(source_info)
        pks_docstring = pks_jinja2_template.render(
            id = source_id,
            title=name,
            description=description,
            url=_get_property(source_info, 'url', default_value="No URL"),
            license=license,
            review=review
        )
        pks_documentation_texts.append(pks_docstring)
    return pks_documentation_texts

def generate_pks_markdown_documentation(pks_documentation_texts):
    pks_jinja2_template = Template("""## {{ title }}
                                   
This page is automatically generated with curated information about primary knowledge sources
leveraged in the MATRIX Knowledge Graph, mainly regarding licensing information and 
potential relevancy assessments for drug repurposing.

This internally curated information is augmented with information from three external resources:

1. [Information Resource Registry](https://biolink.github.io/information-resource-registry/)
2. [reusabledata.org](https://reusabledata.org/)
3. [KG Registry](https://kghub.org/kg-registry/)

{% for doc in pks_documentation_texts %}
{{ doc }}
{% endfor %}
""")

    pks_docs = pks_jinja2_template.render(
        title="Overview of Matrix KG Primary Knowledge Sources",
        pks_documentation_texts=pks_documentation_texts
    )
    return pks_docs

