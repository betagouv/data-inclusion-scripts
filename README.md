# `data-inclusion-scripts`

Ce dépôt contient des workflows pour le traitement des données de l'inclusion.

## Usage

### `data-inclusion validate`

```bash
$ data-inclusion validate --help
Usage: data-inclusion validate [OPTIONS] SRC

  Extract, (transform,) and validate data from a given source

Options:
  --format [csv|json]         [default: json]
  --src-type [dora|standard]  [default: standard]
  --error-output-path PATH
  --help                      Show this message and exit.

# exemples
$ data-inclusion validate --format csv ./examples/valid_data.csv
$ data-inclusion validate --format csv ./examples/invalid_data.csv
```

### `data-inclusion import`

```bash
$ data-inclusion import --help
Usage: data-inclusion import [OPTIONS] SRC DI_API_URL

  Extract, (transform,) validate and load data from a given source to data-
  inclusion

Options:
  --format [csv|json]         [default: json]
  --src-type [dora|standard]  [default: standard]
  -n, --dry-run
  --error-output-path PATH
  --help                      Show this message and exit.
```

Les scripts sont exécutés régulièrement grâce à la ci de github (cf [`.github/workflows/main.yml`](.github/workflows/main.yml)).

## Développement

* Les scripts ont vocation à être via différent points d'entrée (i.e. CLI, Airflow)
* La validation du schéma est effectuée via `pydantic`

## [Auteurs](CODEOWNERS)

## [Licence](LICENSE)
