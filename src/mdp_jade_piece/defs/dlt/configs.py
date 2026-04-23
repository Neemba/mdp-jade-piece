from dlt.common.schema.typing import TColumnSchema
from typing import Dict, Any
from mdp_common.config.table_info import get_table_info as get_common_table_info

DATASET_NAME = "JADE_PIECE"


def get_column_hints(table_name: str) -> Dict[str, TColumnSchema]:
    """
    Génère les hints de colonnes pour forcer les types dans DLT.
    """
    COLUMN_HINTS = {
        "CDEENT": {
            "datedemmodifpo": {"data_type": "text"},
            "datepovalide": {"data_type": "text"},
            "datereceptionpovalide": {"data_type": "text"},
        },
        "DEVENT": {
            "daterelance": {"data_type": "text"},
        },
    }
    return COLUMN_HINTS.get(table_name, {})



TABLE_CLASSIFICATIONS = {
    "CDEENT": {
        "size": "small",
        "description": "Order part from Jade Pieces",
        "target_name": "a_bronze_as400_cdeent",
    },
    "CDEENTBX": {
        "size": "small",
        "description": "Order part from Jade Pieces",
        "target_name": "a_bronze_as400_cdeent_bx",
    },
    "CDELIG": {
        "size": "large",
        "description": "Line order part from Jade Pieces",
        "target_name": "a_bronze_as400_cdelig",
    },
    "CDELIGBX": {
        "size": "large",
        "description": "Line order part from Jade Pieces",
        "target_name": "a_bronze_as400_cdelig_bx",
    },
    "DEVENT": {
        "size": "small",
        "description": "Quote order part from Jade Pieces",
        "target_name": "a_bronze_as400_devent",
    },
    "DEVENTBX": {
        "size": "small",
        "description": "Quote order part from Jade Pieces",
        "target_name": "a_bronze_as400_devent_bx",
    },
    "DEVLIG": {
        "size": "large",
        "description": "Quote part line part from Jade Pieces",
        "target_name": "a_bronze_as400_devlig",
    },
    "DEVLIGBX": {
        "size": "large",
        "description": "Quote part line part from Jade Pieces",
        "target_name": "a_bronze_as400_devlig_bx",
    },
    "FACENTBX": {
        "size": "Medium",
        "description": "Supply from Jade Pieces",
        "target_name": "a_bronze_as400_facent_bx",
    },
    "TIERS": {
        "size": "small",
        "description": "Third party from Jade",
        "target_name": "a_bronze_as400_tiers",
    },
    "FACLIG": {
        "size": "large",
        "description": "Line supply from Jade Pieces",
        "target_name": "a_bronze_as400_faclig",
    },
    "FACLIGBX": {
        "size": "large",
        "description": "Line supply from Jade Pieces",
        "target_name": "a_bronze_as400_faclig_bx",
    },
    "JADIRIUM": {
        "size": "small",
        "description": "Link between concept in Jade & Irium (from Jade)",
        "target_name": "a_bronze_as400_jadirium",
    },
    "LIVENT": {
        "size": "large",
        "description": "Delivery part line part from Jade Pieces",
        "target_name": "a_bronze_as400_livent",
    },
    "LIVENTBX": {
        "size": "large",
        "description": "Delivery part line part from Jade Pieces",
        "target_name": "a_bronze_as400_livent_bx",
    },
    "LIVLIG": {
        "size": "large",
        "description": "Delivery part line part from Jade Pieces",
        "target_name": "a_bronze_as400_livlig",
    },
    "LIVLIGBX": {
        "size": "large",
        "description": "Delivery part line part from Jade Pieces",
        "target_name": "a_bronze_as400_livlig_bx",
    },
}


DATE_COLUMN_MAPPING = {
    "CDEENT": {
        "column": "datemodif", #Au format YYYYMMDD
        "description": "last modification date"
    },
    "DEVENT": {
        "column": "datemodif", #Au format YYYYMMDD
        "description": "last modification date"
    },
    "FACENT": {
        "column": "datemodif", #Au format YYYYMMDD
        "description": "last modification date"
    },
    "LIVENT": {
        "column": "datemodif", #Au format YYYYMMDD
        "description": "last modification date"
    },
}

PRIMARY_KEY_CONFIG = {
    "CDEENT": {
        "keys": ["iddoc"],
        "merge_keys": ["iddoc"], #A changer
        "description": "Order part from Jade Pieces",
    },
    "DEVENT": {
        "keys": ["iddoc"],
        "merge_keys": ["iddoc"], #A changer
        "description": "Quote order part from Jade Pieces",
    }, 
    "CDELIG": {
        "keys": ["IDDOCLIG"],
        "merge_keys": ["IDDOCLIG"],
        "description": "Line order part from Jade Pieces",
    },
    "DEVLIG": {
        "keys": ["IDDOCLIG"],
        "merge_keys": ["IDDOCLIG"],
        "description": "Quote part line part from Jade Pieces",
    },
    "FACENT": {
        "keys": ["iddoc"],
        "merge_keys": ["iddoc"], #A changer ?
        "description": "Supply from Jade Pieces",
    }, 
    "FACLIG": {
        "keys": ["IDDOCLIG"],
        "merge_keys": ["IDDOCLIG"],
        "description": "Line supply from Jade Pieces",
    },
    "TIERS": {
        "keys": ["idtiers"],
        "merge_keys": ["idtiers"], 
        "description": "Third party from Jade",
    },
    "LIVENT": {
        "keys": ["iddoc"],
        "merge_keys": ["iddoc"], #A changer ?
        "description": "Delivery part header from Jade Pieces",
    }, 
    "LIVLIG": {
        "keys": ["IDDOCLIG"],
        "merge_keys": ["IDDOCLIG"],
        "description": "Delivery part line part from Jade Pieces",
    },
}


def get_table_info(table_name: str) -> Dict[str, Any]:
    return get_common_table_info(
        table_name=table_name,
        table_classifications=TABLE_CLASSIFICATIONS,
        date_column_mapping=DATE_COLUMN_MAPPING,
        primary_key_config=PRIMARY_KEY_CONFIG,
    )
