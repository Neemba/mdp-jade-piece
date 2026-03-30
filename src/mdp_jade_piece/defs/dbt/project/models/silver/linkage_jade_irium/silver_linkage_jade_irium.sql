{{ 
    config(
        materialized= "view",
        tags= ["silver", "link_jade_irium"]
    )
}}


with jadirium as (
    select * from {{ source('bronze','a_bronze_as400_jadirium') }}
)

SELECT 
       TYPEDONNEE,
       SANSEQUIVALENCE,
       IDJADE,
       LIBELLEJADE,
       IDIRIUM,
       LIBELLEIRIUM,
       IDPAYS,
       NUMGROUPE,
       LIBELLENONCOMMUN,
       TYPEELEMENT,
       ANNULE,
       PRINCIPAL,
       "SOURCE"
FROM jadirium