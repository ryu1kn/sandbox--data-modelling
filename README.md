
# Sandbox: Data modelling

## Prerequisites

* Python 3.12
* (optional) uv

## Project structure

```mermaid
---
title: Node with text
---
flowchart LR
    n1["sandbox__data_modelling\n(Main project. Root directory)"]
    n2["macro_lib\n(Under /macro-lib)"]
    n3["macro_test\n(Under /macro-test)"]

    n1 -- use --> n2
    n3 -- test --> n2
```

## Setup

```sh
python -m venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Install macro-lib as a package to the main project and the test project.
dbt deps
dbt deps --project-dir macro-test
```

## Usage

```sh
dbt build
```

## Test macros

```sh
dbt test --project-dir macro-test
```

## Scenarios

Subprojects (we call them scenarios) have their directories prefixed with `sce_N__` (abbreviation for "scenario N").

For more details, refer to each scenario's [docs](/docs).

## Upgrading dependencies

```sh
uv pip compile requirements.in -o requirements.txt
uv pip install -r requirements.txt
```
