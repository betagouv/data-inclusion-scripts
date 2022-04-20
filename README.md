# `data-inclusion-scripts`

Ce dépôt contient des workflows pour le traitement des données de l'inclusion.

## Usage

```bash
data-inclusion -vv import {csv|json} <NOM_DE_SOURCE> <URL_OU_CHEMIN_VERS_UN_JEU_DE_DONNEES>

# exemple
data-inclusion -vv import csv local-example-ok ./examples/valid_data.csv
data-inclusion -vv import csv local-example-ko ./examples/invalid_data.csv
```

Les scripts sont exécutés régulièrement via le scheduler de Scalingo. La configuration associée se trouve dans le fichier `./cron.json`.

## Développement

* `great-expectations` est utilisé pour le moment pour la validation du schéma de l'inclusion
* Les scripts ont vocation à être via différent points d'entrée (CLI, Airflow)

## [Auteurs](CODEOWNERS)

## [Licence](LICENSE)
