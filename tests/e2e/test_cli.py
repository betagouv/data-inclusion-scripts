import json
import logging
import textwrap

from click.testing import CliRunner

from data_inclusion.cli.cli import cli


def test_validate(caplog):
    caplog.set_level(logging.INFO)
    runner = CliRunner()

    dataset_content = textwrap.dedent(
        """
[
    {
        "id": "b918e01c-85ed-4e8a-86a1-aa6c73b8df41",
        "typologie": "ASSO",
        "structure_parente": null,
        "nom": "MOBILITE 41",
        "siret": "45025786000038",
        "rna": null,
        "presentation_resume": "Plateforme départementale de mobilité",
        "site_web": "http://www.mobilite41.fr",
        "presentation_detail": "",
        "telephone": "0254838812",
        "courriel": "planning@mobilite41.fr",
        "code_postal": "41200",
        "code_insee": "41194",
        "commune": "ROMORANTIN-LANTHENAY",
        "adresse": "3 B RUE DE PRUNIERS",
        "complement_adresse": "",
        "longitude": 1.727392,
        "latitude": 47.352065,
        "source": "dora",
        "date_maj": "2022-05-04T00:00:00+02:00",
        "lien_source": "https://dora.fabrique.social.gouv.fr/structures/mobilite-41",
        "horaires_ouverture": ""
    }
]
"""
    )

    with runner.isolated_filesystem():
        open("dataset.json", "w").write(dataset_content)
        result = runner.invoke(cli, ["validate", "dataset.json"])
        assert result.exit_code == 0
        assert caplog.messages == [
            "Résultats de la validation:",
            "\t0 erreurs détectées",
            "\t0 lignes non conformes",
            "\t1 lignes conformes",
        ]


def test_preprocess_dora_dataset():
    runner = CliRunner()

    dataset_content = textwrap.dedent(
        """
[
    {
        "siret": "44321878900032",
        "codeSafirPe": null,
        "typology": null,
        "id": "4e6f9f89-aa6f-4425-aaba-2b8f657cd4dd",
        "name": "DUMONT FRANCOIS",
        "shortDesc": "",
        "fullDesc": "",
        "url": "",
        "phone": "",
        "email": "",
        "postalCode": "75018",
        "cityCode": "75118",
        "city": "PARIS 18",
        "department": "75",
        "address1": "5 RUE DU SIMPLON",
        "address2": "",
        "ape": "90.03A",
        "longitude": 2.351892,
        "latitude": 48.893324,
        "creationDate": "2022-05-11",
        "modificationDate": "2022-05-11",
        "source": {
            "value": "porteur",
            "label": "Porteur"
        },
        "linkOnSource": "https://dora.incubateur.net/structures/dumont-francois",
        "services": []
    }
]
"""
    )

    with runner.isolated_filesystem():
        open("dataset.json", "w").write(dataset_content)
        result = runner.invoke(
            cli, ["preprocess", "--src-type", "dora", "dataset.json", "output.json"]
        )
        assert result.exit_code == 0
        result = json.loads(open("output.json", "r").read())
        assert result == [
            {
                "siret": "44321878900032",
                "typologie": None,
                "id": "4e6f9f89-aa6f-4425-aaba-2b8f657cd4dd",
                "nom": "DUMONT FRANCOIS",
                "presentation_resume": None,
                "presentation_detail": None,
                "site_web": None,
                "telephone": None,
                "courriel": None,
                "code_postal": "75018",
                "code_insee": "75118",
                "commune": "PARIS 18",
                "adresse": "5 RUE DU SIMPLON",
                "complement_adresse": None,
                "longitude": 2.351892,
                "latitude": 48.893324,
                "date_maj": "2022-05-10T22:00:00+00:00",
                "source": "dora",
                "lien_source": "https://dora.incubateur.net/structures/dumont-francois",
                "rna": None,
                "structure_parente": None,
                "horaires_ouverture": None,
            }
        ]
