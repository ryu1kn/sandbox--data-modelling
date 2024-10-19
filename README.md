
# Sandbox: Data modelling

## Prerequisites

* Python 3.12
* (optional) uv

## Setup

```sh
python -m venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Usage

```sh
dbt build
```

## Scenarios

Subprojects (we call them scenarios) have their directories prefixed with `sce_N__` (abbreviation for "scenario N").

For more details, refer to each scenario's [docs](/docs).

## Upgrading dependencies

```sh
uv pip compile requirements.in -o requirements.txt
uv pip install -r requirements.txt
```
