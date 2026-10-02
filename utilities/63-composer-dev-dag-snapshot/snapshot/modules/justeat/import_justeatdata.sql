 --DROP PROCEDURE smartdatadb.import_justeatdata(int4);

CREATE OR REPLACE PROCEDURE smartdatadb.import_justeatdata(IN v_import_id integer)
 LANGUAGE plpgsql
AS $procedure$
declare
rec record;
v_objektid int;
--v_justeatdata_extidtyp_place int := 10; ---google_place_id,  
v_justeatdata_extidtyp_data int := 17;  ---lieferando
v_justeatdata_kommtyp int := 1; ---phone
v_justeatdata_objektgruppe int := 1;
v_justeatdata_quelle int := 19; ---source id
v_commit_counter int := 0;
v_error_counter int := 0;
v_error_message text;
v_counter_insert int := 0;
v_counter_update int := 0;
v_error_flag char(10);
v_idx int;

begin
   for rec in (
			WITH justeatdata as
				(
                 SELECT
                   jsondata->>'name' AS business_name,
                   COALESCE(jsondata->>'address_line1', '') || ' ' || COALESCE(jsondata->>'address_line2', '') AS address,
                   jsondata->>'address_line1' as street,
                   jsondata->>'postcode' as postcode,
                   jsondata->>'city' as city,
                   CASE 
                        WHEN jsondata->>'country' = 'Germany' THEN 'DE'
                        WHEN jsondata->>'country' = 'Austria' THEN 'AT'
                        WHEN jsondata->>'country' = 'Switzerland' THEN 'CH'
                        WHEN jsondata->>'country' = 'Belgium' THEN 'BE'
                        WHEN jsondata->>'country' = 'Bulgaria' THEN 'BG'
                        WHEN jsondata->>'country' = 'Czech Republic' THEN 'CZ'
                        WHEN jsondata->>'country' = 'Denmark' THEN 'DK'
                        WHEN jsondata->>'country' = 'France' THEN 'FR'
                        WHEN jsondata->>'country' = 'Italy' THEN 'IT'
                        WHEN jsondata->>'country' = 'Canada' THEN 'CA'
                        WHEN jsondata->>'country' = 'Colombia' THEN 'CO'
                        WHEN jsondata->>'country' = 'Liechtenstein' THEN 'LI'
                        WHEN jsondata->>'country' = 'Netherlands' THEN 'NL'
                        WHEN jsondata->>'country' = 'Luxembourg' THEN 'LU'
                        WHEN jsondata->>'country' = 'Poland' THEN 'PL'
                        WHEN jsondata->>'country' = 'Spain' THEN 'ES'
                        WHEN jsondata->>'country' = 'United Kingdom' THEN 'UK'
                        WHEN jsondata->>'country' = 'United States' THEN 'US'
                        WHEN jsondata->>'country' = 'Croatia' THEN 'HR'
                        WHEN jsondata->>'country' = 'Hungary' THEN 'HU'
                        ELSE 'DE' -- Default to Germany for JustEat DE
                   END as lkz,
                   'Restaurant' AS business_type,
                   NULLIF(jsondata->>'rating', '')::double precision AS rating,
                   NULLIF(jsondata->>'rating_count', '')::integer AS review_count,
                   jsondata->>'website' AS website,
                   jsondata->>'phone' AS phone,
                   NULLIF(jsondata->>'latitude', '')::double precision AS lat,
                   NULLIF(jsondata->>'longitude', '')::double precision AS lng,
                   CASE 
                        WHEN jsondata->>'latitude' IS NOT NULL AND jsondata->>'longitude' IS NOT NULL 
                        THEN ST_GeographyFromText('POINT('|| replace(jsondata->>'longitude', ',', '.') ||' '|| replace(jsondata->>'latitude', ',', '.')||')' )
                        ELSE NULL 
                   END as geopoint,
                   jsondata->>'description' AS description,
                   NULL AS justeatdata_maps_rank,
                   CASE
                       WHEN jsonb_typeof(jsondata->'cuisines') = 'array' THEN
                            array_to_string((SELECT array_agg(x::text)FROM jsonb_array_elements_text(jsondata->'cuisines') AS x),', ')
                       ELSE jsondata->>'cuisines'
                   END AS service_options,
                   COALESCE (jsondata->>'restaurant_id',jsondata->>'slug') AS data_id,
                   jsondata->>NULL AS place_id,--'slug'
                   CASE 
                        WHEN jsondata->>'is_open' = 'true' THEN 'Open'
                        WHEN jsondata->>'is_open' = 'false' THEN 'Closed'
                        ELSE 'Unknown'
                   END AS openstate,
                   'justeat' AS api_type,
                   'json' AS format_type,
                   eingefuegtam,
                   'DE' as country_id,
                   'DE' as country_code,
                   row_number() over (partition by COALESCE (jsondata->>'restaurant_id',jsondata->>'slug')) as rn
                FROM smartdatastagdb.jsonimport p                                                          --JustEat JSON data table
                   WHERE p.spider = 'jeat_de'  -- Filter for JustEat DE spider data
                   AND p.jsondata is not null
                   AND p.jsondata->>'restaurant_id' is not null
                   AND p.jsondata->>'name' is not null
                   AND NOT (p.jsondata->>'is_test_restaurant')::boolean  -- Exclude test restaurants
                   ),

			justeatupsert AS(
			
                   select distinct justeatdata.*, o.objektid,
                        (case when justeatdata.openstate = 'Closed' then true else false end) as openstate_new,
                        (case when o.objektid is null then 'insert' else 'update' end) as operation_objekt,
                        (case when a.objektid is null then 'insert' else 'update' end) as operation_adresse,
                        (case when e20.objektid is null then 'insert'
                              when e20.objektid is not null and e20.extid<>justeatdata.data_id
                              then 'update' else 'nothing' end) as operation_e20,
--                        (case when e10.objektid is null then 'insert'
--                              when e10.objektid is not null and e10.extid<>justeatdata.place_id
--                              then 'update' else 'nothing' end) as operation_e10 ,
                       (case when justeatdata.phone is null then 'nothing'
                             when k1.objektid is null then 'insert'
                             when k1.objektid is not null and k1.text1 = justeatdata.phone then 'nothing'
                             when k1.objektid is not null and k1.text1 <> justeatdata.phone then 'update' end) as operation_phone ,
                       (case when justeatdata.website is null then 'nothing'
                             when justeatdata.website='' then 'nothing'
                             when om154.objektid is null then 'insert'
                             when om154.objektid is not null and coalesce(lower(om154.wert),'') = coalesce(lower(justeatdata.website),'') then 'nothing'
                             when om154.objektid is not null and coalesce(lower(om154.wert),'') <> coalesce(lower(justeatdata.website),'') then 'update' end) as operation_om154 ,
                       (case when justeatdata.description is null then 'nothing'
                             when om269.objektid is null then 'insert'
                             when om269.objektid is not null and coalesce(lower(om269.wert),'') = coalesce(lower(justeatdata.description),'') then 'nothing'
                             when om269.objektid is not null and coalesce(lower(om269.wert),'') <> coalesce(lower(justeatdata.description),'') then 'update' end) as operation_om269 ,
                       (case when justeatdata.review_count is null then 'nothing' 
                             when om299.objektid is null then 'insert'
                             when om299.objektid is not null and coalesce(lower(om299.wert),'') = coalesce(lower(justeatdata.review_count::text),'') then 'nothing'
                             when om299.objektid is not null and coalesce(lower(om299.wert),'') <> coalesce(lower(justeatdata.review_count::text),'') then 'update' end) as operation_om299 ,
                       (case when justeatdata.rating is null then 'nothing'
                             when om374.objektid is null then 'insert'
                             when om374.objektid is not null and coalesce(lower(om374.wert),'') = coalesce(lower(justeatdata.rating::text),'') then 'nothing'
                             when om374.objektid is not null and coalesce(lower(om374.wert),'') <> coalesce(lower(justeatdata.rating::text),'') then 'update' end) as operation_om374 ,
                       (case when justeatdata.business_type is null then 'nothing'
                             when om6.objektid is null then 'insert'
                             when om6.objektid is not null and coalesce(lower(om6.wert),'') = coalesce(lower(justeatdata.business_type),'') then 'nothing'
                             when om6.objektid is not null and coalesce(lower(om6.wert),'') <> coalesce(lower(justeatdata.business_type),'') then 'update' end) as operation_om6 ,
                       (case when justeatdata.justeatdata_maps_rank is null then 'nothing'
                             when om418.objektid is null then 'insert'
                             when om418.objektid is not null and coalesce(lower(om418.wert),'') = coalesce(lower(justeatdata.justeatdata_maps_rank::text),'') then 'nothing'
                             when om418.objektid is not null and coalesce(lower(om418.wert),'') <> coalesce(lower(justeatdata.justeatdata_maps_rank::text),'') then 'update' end) as operation_om418 ,
                       (case when justeatdata.service_options is null then 'nothing'
                             when om419.objektid is null then 'insert'
                             when om419.objektid is not null and coalesce(lower(om419.wert),'') = coalesce(lower(justeatdata.service_options),'') then 'nothing'
                             when om419.objektid is not null and coalesce(lower(om419.wert),'') <> coalesce(lower(justeatdata.service_options),'') then 'update' end) as operation_om419 ,
                       (case when justeatdata.openstate is null then 'nothing'
                             when om394.objektid is null then 'insert'
                             when om394.objektid is not null and coalesce(lower(om394.wert),'') = coalesce(lower(justeatdata.openstate),'') then 'nothing'
                             when om394.objektid is not null and coalesce(lower(om394.wert),'') <> coalesce(lower(justeatdata.openstate),'') then 'update' end) as operation_om394 ,
                       (case when justeatdata.api_type is null then 'nothing'
                             when om421.objektid is null then 'insert'
                             when om421.objektid is not null and coalesce(lower(om421.wert),'') = coalesce(lower(justeatdata.api_type),'') then 'nothing'
                             when om421.objektid is not null and coalesce(lower(om421.wert),'') <> coalesce(lower(justeatdata.api_type),'') then 'update' end) as operation_om421 ,
                       (case when justeatdata.format_type is null then 'nothing'
                             when om422.objektid is null then 'insert'
                             when om422.objektid is not null and coalesce(lower(om422.wert),'') = coalesce(lower(justeatdata.format_type),'') then 'nothing'
                             when om422.objektid is not null and coalesce(lower(om422.wert),'') <> coalesce(lower(justeatdata.format_type),'') then 'update' end) as operation_om422 ,
--                       (case when justeatdata.openstate = 'Closed' then 'insert'
--                             else 'nothing' end) as operation_om378 ,
				
				row_number() over (partition by o.objektid order by justeatdata.data_id) as obj_rn

                from justeatdata

                    left join smartdatadb.externid e20 on justeatdata.data_id = e20.extid and e20.extidtypid = v_justeatdata_extidtyp_data 
--                    left join smartdatadb.externid e10 on justeatdata.place_id = e10.extid and e10.extidtypid = v_justeatdata_extidtyp_place
                    --left join smartdatadb.objekt o on o.objektid = e10.objektid and o.quellenid = v_justeatdata_quelle and o.objektgruppeid = v_justeatdata_objektgruppe
					left join smartdatadb.objekt o on o.objektid = e20.objektid and o.quellenid = v_justeatdata_quelle 
					and o.objektgruppeid = v_justeatdata_objektgruppe
                    left join smartdatadb.adresse a on o.objektid = a.objektid
                    left join smartdatadb.kommunikation k1 on k1.objektid = o.objektid and k1.kommtypid = v_justeatdata_kommtyp -- phone
                    left join smartdatadb.objektmerkmal om154 on o.objektid = om154.objektid and om154.merkmalid = 154      -- website
                    left join smartdatadb.objektmerkmal om269 on o.objektid = om269.objektid and om269.merkmalid = 269      -- description
                    left join smartdatadb.objektmerkmal om299 on o.objektid = om299.objektid and om299.merkmalid = 299      -- rating_count
                    left join smartdatadb.objektmerkmal om374 on o.objektid = om374.objektid and om374.merkmalid = 374      -- justeatdata_rating
                    left join smartdatadb.objektmerkmal om6 on o.objektid = om6.objektid and om6.merkmalid = 6              -- business_type
                    left join smartdatadb.objektmerkmal om418 on o.objektid = om418.objektid and om418.merkmalid = 418      -- justeatdata_maps_rank
                    left join smartdatadb.objektmerkmal om419 on o.objektid = om419.objektid and om419.merkmalid = 419      -- service_options
                    left join smartdatadb.objektmerkmal om394 on o.objektid = om394.objektid and om394.merkmalid = 394      -- openstate --
                    left join smartdatadb.objektmerkmal om421 on o.objektid = om421.objektid and om421.merkmalid = 421      -- api_type --
                    left join smartdatadb.objektmerkmal om422 on o.objektid = om422.objektid and om422.merkmalid = 422      -- format_type
                    --LEFT JOIN smartdatadb.objektmerkmal om378 ON o.objektid = om378.objektid AND om378.merkmalid = 378      -- is closed
                where justeatdata.rn=1      -- Get only one Dataset from External Key for no duplicates.
				)
			select * from justeatupsert where (objektid is null or obj_rn = 1)
			
		)
		
        loop
			
            v_commit_counter := v_commit_counter + 1;
            raise notice '%', v_commit_counter;
			
--This section writes the data into the base tables of the data model.
            begin
 				
                if rec.operation_objekt = 'insert'  then
                 
 				select nextval('smartdatadb.objekt_objektid_seq') into v_objektid;
                    
                    insert into smartdatadb.objekt (objektid, firma1, objektgruppeid, quellenid, eingefuegtam, eingefuegtvon)
                    values (v_objektid, rec.business_name, v_justeatdata_objektgruppe, v_justeatdata_quelle, now(), 'import_justeatdata');
                     
                    v_counter_insert := v_counter_insert + 1;
                 
                elsif rec.operation_objekt = 'update'  then
				raise notice 'value of rec.objektid at objekt is: %',rec.objektid ;
                 
                    update smartdatadb.objekt
                    set name = rec.business_name,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    where objektid = rec.objektid
                    and quellenid = v_justeatdata_quelle
                    and objektgruppeid = v_justeatdata_objektgruppe;
                 
                    v_counter_update := v_counter_update + 1;
                 
                end if;
                 
                if rec.operation_adresse = 'insert'  then
                    
                    insert into smartdatadb.adresse (objektid, adresstypid, strasse, lkz, plz, ort, bundesland, geo_lat, geo_long, eingefuegtam, eingefuegtvon)
                    values (v_objektid, 1, rec.street, rec.lkz, rec.postcode, rec.city, null, rec.lat, rec.lng, now(), 'import_justeatdata');
                                     
                elsif rec.operation_adresse = 'update'  then
				raise notice 'value of rec.objektid at adresse is: %',rec.objektid ;
                 
                    update smartdatadb.adresse
                    set strasse = rec.street, 
                        lkz = rec.lkz,
                        plz = rec.postcode,
                        ort = rec.city,
                        geo_lat = rec.lat,
                        geo_long = rec.lng,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    where objektid = rec.objektid;
                 
                end if;
             
                if rec.operation_e20 = 'insert'  then
                 
                    insert into smartdatadb.externid (objektid, extid, extidtypid, eingefuegtam, eingefuegtvon)
                    values (v_objektid, rec.data_id, v_justeatdata_extidtyp_data, now(), 'import_justeatdata');
                 
                elsif rec.operation_e20 = 'update'  then
				raise notice 'value of rec.objektid ate externid is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.externid
                    SET extid=rec.data_id,
                        extidtypid=v_justeatdata_extidtyp_data,
                        geaendertam=now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid;
                 
                end if;
             
----                if rec.operation_e10 = 'insert'  then
----                 
----                    insert into smartdatadb.externid (objektid, extid, extidtypid, eingefuegtam, eingefuegtvon)
----                    values (v_objektid, rec.place_id, v_justeatdata_extidtyp_place, now(), 'import_justeatdata');
----                 
----                elsif rec.operation_e10 = 'update'  then
----                 
----                    UPDATE smartdatadb.externid
----                    SET extid=rec.place_id,
----                        extidtypid=v_justeatdata_extidtyp_place,
----                        geaendertam=now(),
----                        geaendertvon = 'import_justeatdata'
----                    WHERE objektid=rec.objektid;
----                 
----                end if;
----             
                if rec.operation_phone = 'insert'  then
                 
                    insert into smartdatadb.kommunikation(objektid, kommtypid, text1, eingefuegtam, eingefuegtvon)
                        values (v_objektid, v_justeatdata_kommtyp, rec.phone, now(), 'import_justeatdata');
                 
                elsif rec.operation_phone = 'update'  then
				raise notice 'value of rec.objektid ate kommunikation is: %',rec.objektid ;
				
                 
                    UPDATE smartdatadb.kommunikation
                    SET text1=rec.phone,
                    geaendertam=now(),
                    geaendertvon='import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND kommtypid=v_justeatdata_kommtyp;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om154 = 'insert'  then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 154, rec.website, now(), 'import_justeatdata');
                 

                elsif rec.operation_om154 = 'update'  and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 154 is: %',rec.objektid ;
		
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.website,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=154;
                 
                end if;
             
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om269 = 'insert'  then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 269, rec.description, now(), 'import_justeatdata');
                 
                elsif rec.operation_om269 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 269 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.description,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=269;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om299 = 'insert' then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 299, rec.review_count, now(), 'import_justeatdata');
                 
                elsif rec.operation_om299 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 299 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.review_count,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=299;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om374 = 'insert' then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 374, rec.rating, now(), 'import_justeatdata');
                 
                elsif rec.operation_om374 = 'update' and rec.objektid is not NULL  then
				raise notice 'value of rec.objektid ate 374 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.rating,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=374;
                 
                end if;
             
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om6 = 'insert'  then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 6, rec.business_type, now(), 'import_justeatdata');
                 
                elsif rec.operation_om6 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 6 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.business_type,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=6;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om418 = 'insert'  then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 418, rec.justeatdata_maps_rank, now(), 'import_justeatdata');
                 
                elsif rec.operation_om418 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 418 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.justeatdata_maps_rank,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=418;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om419 = 'insert'  then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 419, rec.service_options, now(), 'import_justeatdata');
                 
                elsif rec.operation_om419 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 419 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.service_options,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=419;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om394 = 'insert' then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 394, rec.openstate, now(), 'import_justeatdata');
                 
                elsif rec.operation_om394 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 394 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.openstate,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=394;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om421 = 'insert'  then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 421, rec.api_type, now(), 'import_justeatdata');
                 
                elsif rec.operation_om421 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 421 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.api_type,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=421;
                 
                end if;
             	
				raise notice 'value of v_objektid is: %',v_objektid ;
                if rec.operation_om422 = 'insert'  then
                 
                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
                       values (v_objektid, 422, rec.format_type, now(), 'import_justeatdata');
                 
                elsif rec.operation_om422 = 'update' and rec.objektid is not NULL then
				raise notice 'value of rec.objektid ate 422 is: %',rec.objektid ;
                 
                    UPDATE smartdatadb.objektmerkmal
                    set wert = rec.format_type,
                        geaendertam = now(),
                        geaendertvon = 'import_justeatdata'
                    WHERE objektid=rec.objektid
                    AND merkmalid=422;
                 
                end if;
             	
--				raise notice 'value of v_objektid is: %',v_objektid ;
--                if rec.operation_om378 = 'insert'  then
--                 
--                    insert into smartdatadb.objektmerkmal (objektid, merkmalid, wert, eingefuegtam, eingefuegtvon)
--                       values (v_objektid, 378, rec.openstate_new, now(), 'import_justeatdata');
--                 
--                elsif rec.operation_om378 = 'update' and rec.objektid is not NULL then
--				raise notice 'value of rec.objektid ate 378 is: %',rec.objektid ;
--                 
--                    UPDATE smartdatadb.objektmerkmal
--                    set wert = rec.openstate_new,
--                        geaendertam = now(),
--                        geaendertvon = 'import_justeatdata'
--                    WHERE objektid=rec.objektid
--                    AND merkmalid=378;
--                 
--                end if;
 
                         
--This section writes the external key and the error message in a logging table
            exception when others then
                begin
                    v_error_message := SQLERRM;
                    v_error_counter := v_error_counter+1;
                    insert into smartdatadb.import_bad_dwh (id, objektid, fehlermeldung, eingefuegtam)
                    values (rec.data_id, v_objektid, v_error_message, now());
                end;
             
            end;
        end loop;
         
        select (case when v_error_counter = 0 then 'N' else 'J' end) into v_error_flag;
-- This section logs the whole importprocess.  (importid, process step, error flag, log_text, inserted_time)
        insert into smartdatastagdb.import_log (importid, prozessschritt, fehlerflag, log_text, eingefuegtam)
        values (v_import_id, 'Prozess: smartdatastagdb.import_justeatdata', v_error_flag, 'Insert: '|| v_counter_insert || ' Update: ' || v_counter_update || ' Error: ' ||v_error_counter, now());
    commit;
    call smartdatadb.jeat_create_master(v_import_id, v_justeatdata_quelle);
end;
$procedure$
;
