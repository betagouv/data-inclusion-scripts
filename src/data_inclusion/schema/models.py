from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, EmailStr, Extra, HttpUrl, constr, root_validator


class Typologie(str, Enum):
    ACI = "ACI"
    ACIPHC = "ACIPHC"
    AFPA = "AFPA"
    AI = "AI"
    ASE = "ASE"
    ASSO = "ASSO"
    ASSO_CHOMEUR = "ASSO_CHOMEUR"
    Autre = "Autre"
    BIB = "BIB"
    CAARUD = "CAARUD"
    CADA = "CADA"
    CAF = "CAF"
    CAP_EMPLOI = "CAP_EMPLOI"
    CAVA = "CAVA"
    CC = "CC"
    CCAS = "CCAS"
    CCONS = "CCONS"
    CD = "CD"
    CHRS = "CHRS"
    CHU = "CHU"
    CIAS = "CIAS"
    CIDFF = "CIDFF"
    CITMET = "CITMET"
    CPH = "CPH"
    CS = "CS"
    CSAPA = "CSAPA"
    DEETS = "DEETS"
    DEPT = "DEPT"
    DIPLP = "DIPLP"
    E2C = "E2C"
    EA = "EA"
    EATT = "EATT"
    EI = "EI"
    EITI = "EITI"
    EPCI = "EPCI"
    EPIDE = "EPIDE"
    ETTI = "ETTI"
    FAIS = "FAIS"
    GEIQ = "GEIQ"
    HUDA = "HUDA"
    MDE = "MDE"
    MDEF = "MDEF"
    MDPH = "MDPH"
    MDS = "MDS"
    MJC = "MJC"
    ML = "ML"
    MQ = "MQ"
    MSA = "MSA"
    MUNI = "MUNI"
    OACAS = "OACAS"
    ODC = "ODC"
    OF = "OF"
    OIL = "OIL"
    OPCS = "OPCS"
    PAD = "PAD"
    PE = "PE"
    PENSION = "PENSION"
    PIJ_BIJ = "PIJ_BIJ"
    PIMMS = "PIMMS"
    PJJ = "PJJ"
    PLIE = "PLIE"
    PREF = "PREF"
    PREVENTION = "PREVENTION"
    REG = "REG"
    RFS = "RFS"
    RS_FJT = "RS_FJT"
    SCP = "SCP"
    SPIP = "SPIP"
    TIERS_LIEUX = "TIERS_LIEUX"
    UDAF = "UDAF"


class Structure(BaseModel):
    id: str
    siret: Optional[constr(min_length=14, max_length=14, regex=r"^\d{14}$")]
    rna: Optional[constr(min_length=10, max_length=10, regex=r"^W\d{9}$")]
    nom: str
    commune: str
    code_postal: constr(min_length=5, max_length=5, regex=r"^\d{5}$")
    # le code_insee est optionnel dans le schéma de données, mais obligatoire pour la
    # consolidation dans data.inclusion
    code_insee: constr(min_length=5, max_length=5)
    adresse: str
    complement_adresse: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    typologie: Optional[Typologie]
    telephone: Optional[str]
    courriel: Optional[EmailStr]
    site_web: Optional[HttpUrl]
    presentation_resume: Optional[constr(max_length=280)]
    presentation_detail: Optional[str]
    source: Optional[str]
    date_maj: date | datetime
    structure_parente: Optional[str]
    lien_source: Optional[HttpUrl]
    horaires_ouverture: Optional[str]

    class Config:
        extra = Extra.forbid

    @root_validator
    def has_pivot_or_reference_to_parent(cls, values: dict) -> dict:
        has_rna = values.get("rna", None) is not None
        has_siret = values.get("siret", None) is not None
        has_reference = values.get("structure_parente", None) is not None
        if not (has_rna or has_siret or has_reference):
            raise ValueError("absence de pivot ou de référence à une structure parente")
        return values
