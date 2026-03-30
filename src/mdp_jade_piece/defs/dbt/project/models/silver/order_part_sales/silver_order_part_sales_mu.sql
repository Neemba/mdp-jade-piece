{{ 
    config(
        materialized= "view",
        tags= ["silver", "sales_order"]
    )
}}


with order_part as (
    select * from {{ ref('silver_order_part_mu') }}
),


doublons_lignes as (        
    SELECT order_part.docnum
        || '-' || order_part.docsuffix1 
        || '-' || order_part.docsuffix2 
        || '-' || coalesce(CAST(order_part.postenum AS VARCHAR), '')
        || '-' || coalesce(CAST(order_part.postesuffix AS VARCHAR), '')
        || '-' || coalesce(trim(order_part.codetrf),'')
        || '-' || coalesce(trim(order_part.refpiece), '')                   
        || '-' || coalesce(CAST(order_part.idligne AS VARCHAR), '')                AS cdelig_cdcl_code
       ,sum(order_part.qtecde - order_part.qteannulee)                              AS cdcl_quantite_commandee
       ,sum(order_part.qtefactot)                                               AS cdcl_quantite_facturee
       ,sum(order_part.qtelivtot)                                               AS cdcl_quantite_livree
       ,count(*)                                                            AS compteur
       ,max(order_part.iddoclig)                                                AS iddoclig
    FROM order_part
    WHERE 1 = 1
       AND ( ( order_part.typeinfo = 'PC'
             AND LEFT(order_part.typeligne, 1) <> 'C' )
              OR order_part.typeinfo = 'FR' )
    GROUP BY order_part.docnum
        || '-' || order_part.docsuffix1 
        || '-' || order_part.docsuffix2 
        || '-' || coalesce(CAST(order_part.postenum AS VARCHAR), '')
        || '-' || coalesce(CAST(order_part.postesuffix AS VARCHAR), '')
        || '-' || coalesce(trim(order_part.codetrf),'')
        || '-' || coalesce(trim(order_part.refpiece), '')                   
        || '-' || coalesce(CAST(order_part.idligne AS VARCHAR), '')

),

Entete as (
    SELECT
       '22' || '-01-' || docnum
        || '-' || docsuffix1 
        || '-' || docsuffix2                                                                 AS cdce_code
       ,'22' || '-01'                                                                               AS cdce_agc_code
       ,NULL                                                                                        AS cdce_cdv_code
       ,devvente                                                                             AS cdce_dev_code
       ,NULL                                                                                        AS cdce_document
       ,'22'                                                                                        AS cdce_dra_code
       ,iddoc                                                                                AS cdce_identifiant_jade_commande_entete
       ,NULL                                                                                        AS cdce_lib_commande
       ,'Jade Maurice'                                                                             AS cdce_module_origine
       ,typefacturation                                                                      AS cdce_nop_code
       ,docnum || '-' || docsuffix1 || '-00'                                          AS cdce_numero_commande
       ,utilisateur                                                                          AS cdce_ope_code
       ,etatdoc                                                                              AS cdce_pocc_code_etat_facturation
       ,Null                                                                                        AS cdce_pocc_code_etat_livraison
       ,null                                                                                        AS cdce_rcu_code
       ,codepromo                                                                            AS cdce_rec_code
       ,refclient                                                                            AS cdce_reference_commande
       ,'300'                                                                                       AS cdce_ser_code_credit
       -- ,cdedet_t.typetarif                                                                          AS cdce_tar_code à retrouver via devise remonter cdedet
       -- ,ROUND(cdedet_d.tauxchangedv ,6)                                                             AS cdce_taux_devise_commande_societe
       /*,'22' || '-'
        || CAST(tie_client.tie_identifiant_source_tiers  as VARCHAR)                                AS cdce_tie_code_client
       ,'22' || '-'
        || CAST(tie_client.tie_identifiant_source_tiers as VARCHAR)                                 AS cdce_tie_code_client_livre*/
       ,classcde || '-' || transport                                                  AS cdce_top_code
       ,datecreat                                                                            AS cdce_tps_date_creation
       ,datemodif                                                                            AS cdce_tps_date_souhaitee
       ,NULL                                                                                        AS cdce_trp_code
    FROM order_part
    WHERE CAST(datemodif AS INTEGER) > TO_NUMBER(TO_CHAR(CURRENT_DATE - 730, 'YYYYMMDD'))
    ),

Lignes as (
    SELECT 
        '22' || '-01-' || docnum
        || '-' || docsuffix1 
        || '-' || docsuffix2 
        || '-' || coalesce(CAST(postenum AS VARCHAR), '')
        || '-' || coalesce(CAST(postesuffix AS VARCHAR), '')
        || '-' || coalesce(trim(codetrf),'')
        || '-' || coalesce(trim(refpiece), '')                   
        || '-' || coalesce(CAST(idligne AS VARCHAR), '')
        || '-' || CAST(order_part.iddoclig AS VARCHAR)                                                             AS cdcl_code
       ,'22' || '-01'                                                                                   AS cdcl_agc_code
       ,'22' || '-01-' || docnum
        || '-' || docsuffix1 
        || '-' || docsuffix2                                                                            AS cdcl_cdce_code
       ,NULL                                                                                            AS cdcl_cdfl_code
       ,coalesce(CAST(postenum AS VARCHAR), '')
        || '-' || coalesce(CAST(postesuffix AS VARCHAR), '')
        || '-' || coalesce(trim(codetrf),'')
        || '-' || coalesce(trim(refpiece), '')                   
        || '-' || coalesce(CAST(idligne AS VARCHAR), '')                                                AS cdcl_code_jade_ligne
       ,'22'                                                                                            AS cdcl_dra_code
       ,'22' || '-01-' || docnum
        || '-' || docsuffix1 
        || '-' || docsuffix2                                                                            AS cdcl_dvice_code
       ,NULL                                                                                            AS cdcl_equ_code
       ,NULL                                                                                            AS cdcl_fac_code
       ,NULL                                                                                            AS cdcl_flag_manage
       ,NULL                                                                                            AS cdcl_flag_vente_exceptionnelle
       ,NULL                                                                                            AS cdcl_ges_code
       ,CASE
          WHEN codetrf IS NULL
                OR trim(codetrf) = ''
                OR LEFT(codetrf, 3) = 'CAT' THEN 'CAT'
          ELSE codetrf
        END                                                                                             AS cdcl_icsm_code
       ,iddoc                                                                                           AS cdcl_identifiant_jade_commande_entete
       ,order_part.iddoclig                                                                             AS cdcl_identifiant_jade_commande_ligne
       ,codetrf                                                                                         AS cdcl_identifiant_jade_tarif           
       ,typeinfo                                                                                        AS cdcl_identifiant_jade_type_info     
       ,NULL                                                                                            AS cdcl_lvfl_code
       ,'Jade Maurice'                                                                                  AS cdcl_module_origine
       ,NULL                                                                                            AS cdcl_nature_contre_marque
       ,docnum
        || '-' || docsuffix1 
        || '-' || docsuffix2                                                                            AS cdcl_numero_commande
       ,NULL                                                                                            AS cdcl_numero_contre_marque
       ,coalesce(CAST(postenum AS VARCHAR), '')
        || '-' || coalesce(CAST(postesuffix AS VARCHAR), '')                                            AS cdcl_numero_ligne
       ,CASE WHEN codetrf IS NULL OR trim(codetrf) = '' OR LEFT(codetrf, 3) = 'CAT' 
               THEN 'CAT' 
               ELSE trim(codetrf) 
             END || '-' || refpiece                                                                     AS cdcl_pdt_code
       ,pxunitachat                                                                                     AS cdcl_pmp
       ,pxunitventedv                                                                                   AS cdcl_prix_net_reel
       ,0                                                                                               AS cdcl_quantite_a_livrer
       ,0                                                                                               AS cdcl_quantite_affectee
       ,0                                                                                               AS cdcl_quantite_attendue
       ,CASE WHEN doublons_lignes.iddoclig IS NULL 
             THEN qtecde - qteannulee  
             ELSE doublons_lignes.cdcl_quantite_commandee
        END                                                                                             AS cdcl_quantite_commandee
       ,0                                                                                               AS cdcl_quantite_disponible
       ,CASE WHEN doublons_lignes.iddoclig IS NULL 
             THEN        qtefactot  
             ELSE doublons_lignes.cdcl_quantite_facturee
       END                                                                                              AS cdcl_quantite_facturee
       ,CASE WHEN doublons_lignes.iddoclig IS NULL 
             THEN        qtelivtot  
             ELSE doublons_lignes.cdcl_quantite_livree 
       END                                                                                              AS cdcl_quantite_livree
       ,-2                                                                                              AS cdcl_tps_ak_date_allocation
       ,typeligne                                                                                       AS cdcl_type_ligne
    FROM order_part
    INNER JOIN doublons_lignes
    ON doublons_lignes.cdelig_cdcl_code = docnum
        || '-' || docsuffix1 
        || '-' || docsuffix2 
        || '-' || coalesce(CAST(postenum AS VARCHAR), '')
        || '-' || coalesce(CAST(postesuffix AS VARCHAR), '')
        || '-' || coalesce(trim(codetrf),'')
        || '-' || coalesce(trim(refpiece), '')                   
        || '-' || coalesce(CAST(idligne AS VARCHAR), '')
    AND doublons_lignes.iddoclig = order_part.iddoclig 
    WHERE  1 = 1
        AND ( ( typeinfo = 'PC'
               AND LEFT(typeligne, 1) <> 'C' )
              OR typeinfo = 'FR' )
     AND doublons_lignes.cdelig_cdcl_code IS not null
),

silver_sales_order_part as (
    SELECT DISTINCT 
    cdce_code
    ,cdce_agc_code
    ,cdce_cdv_code
    ,cdce_dev_code
    ,cdce_document
    ,cdce_dra_code
    ,cdce_identifiant_jade_commande_entete
    ,cdce_lib_commande
    ,cdce_module_origine
    ,cdce_nop_code
    ,cdce_numero_commande
    ,cdce_ope_code
    ,cdce_pocc_code_etat_facturation
    ,cdce_pocc_code_etat_livraison
    ,cdce_rcu_code
    ,cdce_rec_code
    ,cdce_reference_commande
    ,cdce_ser_code_credit
    --,cdce_tie_code_client
    --,cdce_tie_code_client_livre
    ,cdce_top_code
    ,cdce_tps_date_creation
    ,cdce_tps_date_souhaitee
    ,cdce_trp_code
    ,cdcl_code
    ,cdcl_agc_code
    ,cdcl_cdce_code
    ,cdcl_cdfl_code
    ,cdcl_code_jade_ligne
    ,cdcl_dra_code
    ,cdcl_dvice_code
    ,cdcl_equ_code
    ,cdcl_fac_code
    ,cdcl_flag_manage
    ,cdcl_flag_vente_exceptionnelle
    ,cdcl_ges_code
    ,cdcl_icsm_code
    ,cdcl_identifiant_jade_commande_entete
    ,cdcl_identifiant_jade_commande_ligne
    ,cdcl_identifiant_jade_tarif           
    ,cdcl_identifiant_jade_type_info     
    ,cdcl_lvfl_code
    ,cdcl_module_origine
    ,cdcl_nature_contre_marque
    ,cdcl_numero_commande
    ,cdcl_numero_contre_marque
    ,cdcl_numero_ligne
    ,cdcl_pdt_code
    ,cdcl_pmp
    ,cdcl_prix_net_reel
    ,cdcl_quantite_a_livrer
    ,cdcl_quantite_affectee
    ,cdcl_quantite_attendue
    ,cdcl_quantite_commandee
    ,cdcl_quantite_disponible
    ,cdcl_quantite_facturee
    ,cdcl_quantite_livree
    ,cdcl_tps_ak_date_allocation
    ,cdcl_type_ligne
    /* =====================================================
       Métadonnées
       ===================================================== */
        ,'JPM'                                              AS application_code,
        'Jade Maurice'                                     AS source_module,
        'sales'                                        AS sales_domain,
        CURRENT_TIMESTAMP()                                 AS dbt_processed_at,
        'silver_order_part_sales_mu'                                 AS dbt_model_name,
        '{{ var("silver_prefix") }}'                        AS layer_prefix

    from Lignes 
    inner join Entete
        on cdce_identifiant_jade_commande_entete = cdcl_identifiant_jade_commande_entete
)

select * from silver_sales_order_part
