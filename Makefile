

.PHONY: all download-sources
all: download-sources build

download-sources:
	curl "https://raw.githubusercontent.com/biolink/information-resource-registry/refs/heads/main/infores_catalog.yaml" > sources/infores_catalog.yaml
	curl "https://raw.githubusercontent.com/reusabledata/reusabledata/refs/heads/master/data.json" > sources/reusabledata.json
	curl "https://raw.githubusercontent.com/Knowledge-Graph-Hub/kg-registry/refs/heads/main/registry/kgs.yml" > sources/kgregistry.yml
	curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vQxpQU80dpW9bo7STfrX7k9Wv70jA_2C4BN6tDceM1LEOfF9YL22OisdmaUPf7Ptw/pub?gid=135786799&single=true&output=tsv" > sources/matrixcurated.tsv
	curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vQxpQU80dpW9bo7STfrX7k9Wv70jA_2C4BN6tDceM1LEOfF9YL22OisdmaUPf7Ptw/pub?gid=1308154629&single=true&output=tsv" > sources/matrixreviews.tsv

build:
	uv run jupyter nbconvert --to notebook --execute kg-source-data.ipynb