import pandas as pd
import pytest

from data_inclusion.tasks.sources import siao


@pytest.fixture
def siao_sample_df():
    return pd.DataFrame(
        [
            {
                "Nom de la structure": "PJJ MILIEU OUVERT Montargis",
                "Type": "INSERTION",
                "Catégorie de structure": "PREMIER_ACCUEIL",
                "Structure partagée": "NON",
                "Territoires": "Loiret",
                "Structure éphémère": "NON",
                "Adresse": "30 Rue GAMBETTA",
                "Ville": "Montargis",
                "Code postal": "45200.0",
                "CodeEPCI": "244500203.0",
                "Département": "45.0",
                "Code INSEE": "           ",
                "Code SIRET": "174 501 312 00204",
                "Nom du référent": "x",
                "Prénom du référent": "x",
                "Téléphone": "02 38 93 90 91",
                "Fax": "03 29 86 29 45",
                "Mail": "vicky.fery-berton@justice.fr",
                "Affectation directe des places": "OUI",
                "Places variables": "NON",
                "Validation automatique": "NON",
                "Accès": "Infirmerie.pietat@korian.fr\r\n05 62 33 86 93",
                "Observation": "Redevance mensuelle, dépend de la taille du logement.",
                "Participation financière nuit": "OUI",
                "Coût nuit": "0.0",
                "Restauration": "NON",
                "Coût restauration": "0.0",
                "Financement P177": "OUI",
                "Nombre de places financées": "0.0",
                "Admission directe": "NON",
                "Coût petit déjeuner": "0.0",
                "Coût déjeuner": "0.0",
                "Coût dîner": "0.0",
                "FINESS": "550006167.0",
                "SYPLO": "                              ",
                "Entitée gestionnaire": "Une Famille Un Toit",
                "Dispositif": "Hébergement",
                "Âge des personnes": "25 à 59 ans, 60 ans et plus, 18 à 24 ans",
                'Public accueilli "Accueil tout public"': "OUI",
                'Public accueilli "Homme seul"': "OUI",
                'Public accueilli "Femme seule"': "OUI",
                'Public accueilli "Couple sans enfant"': "OUI",
                'Public accueilli "Femme seule avec enfant(s)"': "OUI",
                'Public accueilli "Homme seul avec enfant(s)"': "OUI",
                'Public accueilli "Groupe avec enfant(s)"': "OUI",
                'Public accueilli "Groupe d\'adultes sans enfant"': "OUI",
                'Public accueilli "Couple avec enfant"': "OUI",
                'Public accueilli "Femmes victimes de violences"': "OUI",
                'Public accueilli "Enfant / Mineur isolé"': "OUI",
                'Public accueilli "Enfants / Mineurs en groupe"': "OUI",
                'Public accompagné "Accueil tout public"': "OUI",
                'Public accompagné "Jeunes majeurs (18-25 ans)"': "OUI",
                'Public accompagné "Personnes âgées"': "OUI",
                'Public accompagné "Justice"': "OUI",
                'Public accompagné "Pathologies médicales chroniques"': "OUI",
                'Public accompagné "Addictions"': "OUI",
                'Public accompagné "Prostitution"': "OUI",
                'Public accompagné "Personnes victimes de violence"': "OUI",
                'Public accompagné "Troubles psychiatriques"': "OUI",
                'Jour d\'ouverture "Lundi"': "NON",
                'Heure Ouverture "Lundi"': "09:00:00",
                'Heure Fermeture "Lundi"': "17:00:00",
                'Jour d\'ouverture "Mardi"': "NON",
                'Heure Ouverture "Mardi"': "09:00:00",
                'Heure Fermeture "Mardi"': "17:00:00",
                'Jour d\'ouverture "Mercredi"': "NON",
                'Heure Ouverture "Mercredi"': "09:00:00",
                'Heure Fermeture "Mercredi"': "17:00:00",
                'Jour d\'ouverture "Jeudi"': "NON",
                'Heure Ouverture "Jeudi"': "09:00:00",
                'Heure Fermeture "Jeudi"': "17:00:00",
                'Jour d\'ouverture "Vendredi"': "NON",
                'Heure Ouverture "Vendredi"': "09:00:00",
                'Heure Fermeture "Vendredi"': "17:00:00",
                'Jour d\'ouverture "Samedi"': "NON",
                'Heure Ouverture "Samedi"': "09:00:00",
                'Heure Fermeture "Samedi"': "17:00:00",
                'Jour d\'ouverture "Dimanche"': "NON",
                'Heure Ouverture "Dimanche"': "09:00:00",
                'Heure Fermeture "Dimanche"': "17:00:00",
                "Animaux acceptés": "NON",
                "Heure de présentation": "2019-09-09 12:00:46",
                "Début de la présentation": "16:00:00",
                "Fin de la présentation": "18:00:00",
                "Capacité théorique totale": "0.0",
                "Nombre total de places fermées": "0.0",
                "Capacité disponible totale": "0.0",
                "Capacité ouverte totale": "0.0",
                "Nombre de places occupées": "0.0",
            }
        ]
    )


def test_transform_siao(siao_sample_df):
    df = siao.transform_dataframe(siao_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": None,
            "siret": "17450131200204",
            "rna": None,
            "nom": "PJJ MILIEU OUVERT Montargis",
            "commune": "Montargis",
            "code_postal": "45200",
            "code_insee": None,
            "adresse": "30 Rue GAMBETTA",
            "complement_adresse": None,
            "longitude": None,
            "latitude": None,
            "typologie": "PJJ",
            "telephone": "02 38 93 90 91",
            "courriel": "vicky.fery-berton@justice.fr",
            "site_web": None,
            "presentation_resume": None,
            "presentation_detail": None,
            "source": "siao",
            "date_maj": None,
            "structure_parente": None,
            "lien_source": None,
            "horaires_ouverture": None,
            "accessibilite": None,
            "labels_nationaux": [],
            "labels_autres": None,
        }
    ]
