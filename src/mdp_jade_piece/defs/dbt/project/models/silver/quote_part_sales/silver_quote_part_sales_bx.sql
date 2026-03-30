{{ 
    config(
        materialized= "view",
        tags= ["silver", "sales_quote"]
    )
}}


with quote_part as (
    select * from {{ ref('silver_quote_part_bx') }}
),


Entete as (
    SELECT '27'
              || '-01-'
              || docnum
              || '-'
              || docsuffix1
              || '-'
              || docsuffix2 AS dvice_code ,
       '27'
              || '-01' AS dvice_agc_code ,
       NULL            AS dvice_cdv_code ,
       devvente        AS dvice_dev_code ,
       NULL            AS dvice_document ,
       NULL            AS dvice_dos_code ,
       '27'            AS dvice_dra_code ,
       NULL            AS dvice_dvice_code_initial ,
       NULL            AS dvice_dvice_version ,
       iddoc           AS dvice_identifiant_jade_devis_entete ,
       NULL            AS dvice_lib_commande ,
       NULL            AS dvice_lob_code ,
       'Jade Bordeaux' AS dvice_module_origine ,
       typefacturation AS dvice_nop_code ,
       docnum
              || '-'
              || docsuffix1
              || '-'
              || docsuffix2 AS dvice_numero_devis ,
       0                    AS dvice_numero_intervention ,
       utilisateur          AS dvice_ope_code ,
       CASE etatdoc
              WHEN 'ENV' THEN 'EN'
              WHEN 'VAL' THEN 'TR'
              WHEN 'ANN' THEN 'AN'
              WHEN 'TRT' THEN 'EN'
              WHEN 'PER' THEN 'RE'
              ELSE etatdoc
       END       AS dvice_pocc_code ,
       NULL      AS dvice_rec_code_2 ,
       NULL      AS dvice_rec_code_1 ,
       refclient AS dvice_reference_commande ,
       '300'     AS dvice_ser_code_credit ,
       -- ,cdedet_t.typetarif                                                                          AS dvice_tar_code à retrouver via devise remonter cdedet
       NULL AS dvice_taux_devise_commande_societe , -- Voir chargement commande ?
       /*,'27' || '-'
|| CAST(tie_client.tie_identifiant_source_tiers  as VARCHAR)                                AS dvice_tie_code_client
,'27' || '-'
|| CAST(tie_client.tie_identifiant_source_tiers as VARCHAR)                                 AS dvice_tie_code_client_livre*/
       CASE transport
              WHEN 'air' THEN 'AIR'
              WHEN 'mer' THEN 'SUR'
              WHEN 'expre' THEN 'EXP'
              WHEN '<n/a>' THEN 'TOU'
              WHEN 'multi' THEN 'TOU'
              WHEN 'terre' THEN 'RS'
              ELSE transport
       END AS dvice_top_code ,
       datecreat       AS dvice_tps_ak_date_creation ,
       -2              AS dvice_tps_ak_date_demande_derniere_actualisation ,
       datedem         AS dvice_tps_ak_date_demande_initiale ,
       -2              AS dvice_tps_ak_date_envoi_derniere_actualisation ,
       dateenvoi       AS dvice_tps_ak_date_envoi_initial ,
       datelimit       AS dvice_tps_ak_date_limite_validite ,
       datemodif       AS dvice_tps_ak_date_modification ,
       datereceptionpo AS dvice_tps_ak_date_reception ,
       daterelance     AS dvice_tps_ak_date_relance ,
       -2              AS dvice_tps_ak_date_souhaitee ,
       datevalid       AS dvice_tps_ak_date_validation ,
       NULL            AS dvice_trp_code
FROM   quote_part
WHERE  Cast(datemodif AS INTEGER) > To_number(To_char(CURRENT_DATE - 730, 'YYYYMMDD')) ), 

lignes AS
(
       SELECT '27'
                     || '-01-'
                     || docnum
                     || '-'
                     || docsuffix1
                     || '-'
                     || docsuffix2
                     || '-'
                     || COALESCE(cast(postenum AS varchar), '')
                     || '-'
                     || COALESCE(cast(postesuffix AS varchar), '')
                     || '-'
                     || COALESCE(trim(codetrf),'')
                     || '-'
                     || COALESCE(trim(refpiece), '')
                     || '-'
                     || COALESCE(cast(idligne AS varchar), '') AS dvicl_code ,
              '27'
                     || '-01' AS dvicl_agc_code ,
              '27'            AS dvicl_dra_code ,
              '27'
                     || '-01-'
                     || docnum
                     || '-'
                     || docsuffix1
                     || '-'
                     || docsuffix2 AS dvicl_dvice_code ,
              NULL                 AS dvicl_fac_code ,
              CASE
                     WHEN codetrf IS NULL
                     OR     trim(codetrf) = ''
                     OR     LEFT(codetrf, 3) = 'CAT' THEN 'CAT'
                     ELSE codetrf
              END             AS dvicl_icsm_code ,
              iddoc           AS dvicl_identifiant_jade_devis_entete ,
              iddoclig        AS dvicl_identifiant_jade_devis_ligne ,
              'Jade Bordeaux' AS dvicl_module_origine ,
              0               AS dvicl_montant_remise_1 ,
              0               AS dvicl_montant_remise_2 ,
              COALESCE(cast(postenum AS varchar), '')
                     || '-'
                     || COALESCE(cast(postesuffix AS varchar), '')
                     || '-'
                     || COALESCE(trim(codetrf),'')
                     || '-'
                     || COALESCE(trim(refpiece), '')
                     || '-'
                     || COALESCE(cast(idligne AS varchar), '') AS dvicl_numero_ligne ,
              CASE
                     WHEN codetrf IS NULL
                     OR     trim(codetrf) = ''
                     OR     LEFT(codetrf, 3) = 'CAT' THEN 'CAT'
                     ELSE trim(codetrf)
              END
                     || '-'
                     || refpiece AS dvicl_pdt_code ,
              pxunitachat        AS dvicl_prix_net_reel ,
              pxunitventedv      AS dvicl_prix_vente_ht ,
              NULL               AS dvicl_quantite_attendue ,
              qtecde             AS dvicl_quantite_commandee ,
              NULL               AS dvicl_type_ligne
       FROM   quote_part
    WHERE  1 = 1
       AND ( ( Coalesce(typeinfo, '') = 'PC'
               AND LEFT(Coalesce(typeligne, 'ZZ'), 1) <> 'C' )
              OR Coalesce(typeinfo, '') = 'FR' )
),

silver_sales_quote_part as (
    SELECT DISTINCT 
        dvice_code,
        dvice_agc_code ,
        dvice_cdv_code ,
        dvice_dev_code ,
        dvice_document ,
        dvice_dos_code ,
        dvice_dra_code ,
        dvice_dvice_code_initial ,
        dvice_dvice_version ,
        dvice_identifiant_jade_devis_entete ,
        dvice_lib_commande ,
        dvice_lob_code ,
        dvice_module_origine ,
        dvice_nop_code ,
        dvice_numero_devis ,
        dvice_numero_intervention ,
        dvice_ope_code ,
        dvice_pocc_code ,
        dvice_rec_code_2 ,
        dvice_rec_code_1 ,
        dvice_reference_commande ,
        dvice_ser_code_credit ,
        dvice_taux_devise_commande_societe ,
        dvice_top_code ,
        dvice_tps_ak_date_creation ,
        dvice_tps_ak_date_demande_derniere_actualisation ,
        dvice_tps_ak_date_demande_initiale ,
        dvice_tps_ak_date_envoi_derniere_actualisation ,
        dvice_tps_ak_date_envoi_initial ,
        dvice_tps_ak_date_limite_validite ,
        dvice_tps_ak_date_modification ,
        dvice_tps_ak_date_reception ,
        dvice_tps_ak_date_relance ,
        dvice_tps_ak_date_souhaitee ,
        dvice_tps_ak_date_validation ,
        dvice_trp_code ,
        dvicl_code ,
        dvicl_agc_code ,
        dvicl_dra_code ,
        dvicl_dvice_code ,
        dvicl_fac_code ,
        dvicl_icsm_code ,
        dvicl_identifiant_jade_devis_entete ,
        dvicl_identifiant_jade_devis_ligne ,
        dvicl_module_origine ,
        dvicl_montant_remise_1 ,
        dvicl_montant_remise_2 ,
        dvicl_numero_ligne ,
        dvicl_pdt_code ,
        dvicl_prix_net_reel ,
        dvicl_prix_vente_ht ,
        dvicl_quantite_attendue ,
        dvicl_quantite_commandee ,
        dvicl_type_ligne

        /* =====================================================
       Métadonnées
       ===================================================== */
        ,'JPB'                                              AS application_code,
        'Jade Bordeaux'                                     AS source_module,
        'sales'                                        AS sales_domain,
        CURRENT_TIMESTAMP()                                 AS dbt_processed_at,
        'silver_quote_part_sales_bx'                                 AS dbt_model_name,
        '{{ var("silver_prefix") }}'                        AS layer_prefix

    from Lignes 
    inner join Entete
        on dvice_identifiant_jade_devis_entete = dvicl_identifiant_jade_devis_entete
)

select * from silver_sales_quote_part
