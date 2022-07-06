# `data-inclusion-scripts`

Ce dépôt contient des workflows pour le traitement des données de l'inclusion.

## Usage

### `data-inclusion preprocess`

```bash
$ data-inclusion preprocess --help
Usage: data-inclusion preprocess [OPTIONS] SRC OUTPUT_PATH

  Extract from the given datasource and reshape the data to data.inclusion
  format.

Options:
  --format [csv|json]        [default: json]
  --src-type [dora|itou|v0]  [default: v0]
  --help                     Show this message and exit.
```

### `data-inclusion geocode`

```bash
$ data-inclusion geocode --help
Usage: data-inclusion geocode [OPTIONS] FILEPATH OUTPUT_PATH

  Geocode a data file that should be structured in the data.inclusion format.

Options:
  --help  Show this message and exit.
```

### `data-inclusion validate`

```bash
$ data-inclusion validate --help
Usage: data-inclusion validate [OPTIONS] FILEPATH

  Validate a data file that should be structured in the data.inclusion format.

Options:
  --error-output-path PATH
  --help                    Show this message and exit.
```

### `data-inclusion import`

```bash
$ data-inclusion import --help
Usage: data-inclusion import [OPTIONS] SRC

  Extract, (transform,) validate and load data from a given source to data-
  inclusion.

Options:
  --format [csv|json]        [default: json]
  --src-type [dora|itou|v0]  [default: v0]
  -n, --dry-run
  --error-output-path PATH
  --help                     Show this message and exit.
```

Les scripts sont exécutés régulièrement grâce à la ci de github (cf [`.github/workflows/main.yml`](.github/workflows/main.yml)).

## Développement

* Les scripts ont vocation à être via différent points d'entrée (i.e. CLI, Airflow)
* La validation du schéma est effectuée via `pydantic`

## [Auteurs](CODEOWNERS)

## [Licence](LICENSE)
