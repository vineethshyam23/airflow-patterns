-- DROP PROCEDURE smartdatadb.create_justeat_temp_table();

CREATE OR REPLACE PROCEDURE smartdatadb.create_justeat_temp_table()
 LANGUAGE plpgsql
AS $procedure$DECLARE
    json_record RECORD ;

	BEGIN
    	RAISE NOTICE 'Starting justeat create temp table procedure';

	    CREATE TABLE IF NOT EXISTS   smartdatadb.justeat_temp
	                 (
	                              -- Primary key for the table, ensuring each restaurant has a unique identifier.
	                              restaurant_id varchar(255) NOT NULL,
	                              -- General restaurant information
	                              NAME    varchar(255) NOT NULL,
	                              slug    varchar(255) NOT NULL,
	                              phone   varchar(50),
	                              is_open boolean,
	                              website text,
	                              menu_url text,
	                              menu_source_url text,
	                              rest_source_url text,
	                              -- Location data
	                              city     varchar(100),
	                              country  varchar(100),
	                              zipcode  varchar(20),
	                              postcode varchar(20),
	                              address_line1 text,
	                              address_line2 text,
	                              latitude DOUBLE PRECISION,
	                              longitude DOUBLE PRECISION,
	                              -- Business characteristics
	                              is_brand           boolean,
	                              is_chain           boolean,
	                              is_test_restaurant boolean,
	                              -- Delivery and pricing details
	                              delivery_fee            decimal(10, 2),
	                              minimum_order           decimal(10, 2),
	                              free_delivery_threshold decimal(10, 2),
	                              delivery_time text,
	                              -- Rating and review data
	                              rating             decimal(3, 2),
	                              rating_count       integer,
	                              rating_out_of_five decimal(3, 2),
	                              -- Categorization and menu
	                              cuisines text,   -- Stored as an array of strings
	                              categories text, -- Stored as an array of strings
	                              -- Scraping and data processing metadata
	                              request_id                     varchar(255),
	                              spider                         varchar(255),
	                              rest_scraped_at timestamp WITH time zone,
	                              rest_processing_status         varchar(50),
	                              rest_error_message text,
	                              menu_url_extraction_status varchar(50),
	                              created_at timestamp WITH  time zone
	                 );
	    
	    RAISE NOTICE 'justeat temp table creation completed';

		RAISE NOTICE 'deleting existing data';
	    delete FROM   smartdatadb.justeat_temp;
	    RAISE NOTICE 'All the rows deleted from smartdatastagdb.justeat_temp ';

	    INSERT INTO smartdatadb.justeat_temp
	                  (
	                              restaurant_id,
	                              NAME,
	                              slug,
	                              phone,
	                              is_open,
	                              website,
	                              menu_url,
	                              menu_source_url,
	                              rest_source_url,
	                              city,
	                              country,
	                              zipcode,
	                              postcode,
	                              address_line1,
	                              address_line2,
	                              latitude,
	                              longitude,
	                              is_brand,
	                              is_chain,
	                              is_test_restaurant,
	                              delivery_fee,
	                              minimum_order,
	                              free_delivery_threshold,
	                              delivery_time,
	                              rating,
	                              rating_count,
	                              rating_out_of_five,
	                              cuisines,
	                              categories,
	                              request_id,
	                              spider,
	                              rest_scraped_at,
	                              rest_processing_status,
	                              rest_error_message,
	                              menu_url_extraction_status,
	                              created_at
	                  )
	      SELECT jsondata->>'restaurant_id'                           ,
	             jsondata->>'name'                                    ,
	             jsondata->>'slug'                                    ,
	             jsondata->>'phone'                                   ,
	             (jsondata->>'is_open')::boolean                      ,
	             jsondata->>'website'                                 ,
	             jsondata->>'menu_url'                                ,
	             jsondata->>'menu_source_url'                         ,
	             jsondata->>'rest_source_url'                         ,
	             jsondata->>'city'                                    ,
	             jsondata->>'country'                                 ,
	             jsondata->>'zipcode'                                 ,
	             jsondata->>'postcode'                                ,
	             jsondata->>'address_line1'                           ,
	             jsondata->>'address_line2'                           ,
	             NULLIF(jsondata->>'latitude', '')::DOUBLE PRECISION  ,
	             NULLIF(jsondata->>'longitude', '')::DOUBLE PRECISION ,
	             (jsondata->>'is_brand')::                         boolean         ,
	             (jsondata->>'is_chain')::                         boolean         ,
	             (jsondata->>'is_test_restaurant')::               boolean         ,
	             NULLIF(jsondata->>'delivery_fee', '')::           numeric         ,
	             NULLIF(jsondata->>'minimum_order', '')::          numeric         ,
	             NULLIF(jsondata->>'free_delivery_threshold', '')::numeric         ,
	             jsondata->>'delivery_time'                                        ,
	             NULLIF(jsondata->>'rating', '')::            numeric              ,
	             NULLIF(jsondata->>'rating_count', '')::      integer              ,
	             NULLIF(jsondata->>'rating_out_of_five', '')::numeric              ,
	             CASE
	                    WHEN jsonb_typeof(jsondata->'cuisines') = 'array' THEN array_to_string(
	                           (
	                                  SELECT array_agg(x::text)
	                                  FROM   jsonb_array_elements_text(jsondata->'cuisines') AS x),', ')
	                    ELSE jsondata->>'cuisines'
	             END                                                                 ,
	             jsondata->'categories'                                              ,
	             jsondata->>'request_id'                                             ,
	             jsondata->>'spider'                                                 ,
	             (jsondata->>'rest_scraped_at')::timestamptz                         ,
	             jsondata->>'rest_processing_status'                                 ,
	             jsondata->>'rest_error_message'                                     ,
	             jsondata->>'menu_url_extraction_status'                             ,
	             to_timestamp(NULLIF(jsondata->>'_timestamp', '')::DOUBLE PRECISION) 
	      FROM  (
	                    SELECT jsondata::jsonb
	                    FROM   smartdatastagdb.jsonimport
	                    WHERE  jsondata->>'restaurant_id' IS NOT NULL )z ;
	      
	   	RAISE NOTICE 'Procedure create_justeat_temp_table completed Successfully';

EXCEPTION WHEN OTHERS THEN
RAISE NOTICE 'Error: %', SQLERRM;
commit;
end;
$procedure$
;
