
/* Komplettdurchlauf */
REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menuitemsjoin;
REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menuitemsall;

/* Refresh Masterobjekt-Country-Zuordnung für Menuitemsbest */
REFRESH MATERIALIZED VIEW smartdatadb.mv_masterobjekt_country;

REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menuitemsbest;

REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menuitems_cntneu;
/* ausgelagert in extra Prozess wg. Laufzeit */
--call smartdatastagdb.refresh_masterobjekt_keyword_match();

REFRESH MATERIALIZED VIEW smartdatadb.mv_url_score;
REFRESH MATERIALIZED VIEW smartdatastagdb.mv_quality_checkbase;
CALL smartdatastagdb.create_masterobjekt_rating_tables();

REFRESH MATERIALIZED VIEW smartdatastagdb.mv_r_index;
REFRESH MATERIALIZED VIEW smartdatastagdb.mv_d_index;

REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menufilterall;
REFRESH MATERIALIZED VIEW smartdatadb.mv_masterobjektid_uuid;--Added during bug fix
REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menuitems_final;

REFRESH MATERIALIZED VIEW smartdatastagdb.mv_establishment_distances_export;

REFRESH MATERIALIZED VIEW smartdatastagdb.mv_cuisine_type;

SET search_path TO public;
call smartdatastagdb.refresh_poi_densities(1);
call smartdatastagdb.refresh_supermarket_densities(1);
call smartdatastagdb.refresh_discounter_densities(1);
call smartdatastagdb.refresh_competitor_densities(1);

REFRESH MATERIALIZED VIEW smartdatastagdb.mv_city_prices;
