{{ 
    config(
        materialized= "view",
        tags= ["silver", "sales_order"]
    )
}}


with order_part_bx as (
    select * from {{ ref('silver_order_part_sales_bx') }}
),

order_part_mu as (
    select * from {{ ref('silver_order_part_sales_mu') }}
),


union_bx_mu as (        
    SELECT
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
        ,application_code
        ,source_module
        ,sales_domain
        ,dbt_processed_at
        ,dbt_model_name
        ,layer_prefix

    from order_part_bx

    union all

    SELECT
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
        ,application_code
        ,source_module
        ,sales_domain
        ,dbt_processed_at
        ,dbt_model_name
        ,layer_prefix

    from order_part_mu
)

select * from union_bx_mu
