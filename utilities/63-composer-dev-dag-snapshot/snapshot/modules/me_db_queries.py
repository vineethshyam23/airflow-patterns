from .me_db_constants import TableNames, FunctionCalls
from .me_config import MeExtractorConfig
me_config = MeExtractorConfig()


class DbReturnfunct:
    """The generalized db class from me_db.py can execute any query and
    return the result in any structure, using a query and returning 
    using a return function. 
    This is the class for returning the correct function for the query."""
    @staticmethod
    def fetch_columns(d):
        """Returns each column as an item in a list"""
        yield [c for c in d]

    @staticmethod
    def fetch_column_item(d):
        """Returns the first item in 
        each column as an item in a list"""
        yield [c[0] for c in d]

    count_documents = fetch_columns
    fetch_keywords = fetch_column_item
    fetch_link_keywords = fetch_column_item

    @staticmethod
    def fetch_menu_items(d): 
        """Returns the first 4 items in the result 
        as an item in a list, each"""
        yield [d[0], d[1], d[2], d[3], d[4], d[5]]
    
    @staticmethod
    def fetch_documents(d):
        """Returns the first 6 items in the result as 
        an item in a list, each; converts the 5th item
        to bytes"""
        yield [d[0], d[1], d[2], d[3], d[4].tobytes(), d[5]]
    
    fetch_blob_documents = fetch_documents
    
    @staticmethod
    def fetch_all_sub(d):
        """Returns the first item within the first item only"""
        yield d[0][0]


class DbCountingQueries:
    """Query string definitions for the queries 
    that count the number records for any object 
    in the database"""
    def __init__(self, source_doctype='html'):
        if source_doctype == 'html':
            # table_menu_items_raw = TableNames.menuitems_raw
            table_b2b_urls = TableNames.B2B_URLS
            # Table_menuitemsclean_stage = TableNames.menuitemsclean_stage
            # Table_menuitemsclean_new = TableNames.menuitemsclean_new
        elif source_doctype in ['pdf', 'dishpdf']:
            # table_menu_items_raw = TableNames.menuitems_raw_pdf
            table_b2b_urls = TableNames.B2B_URLS_PDF
            # Table_menuitemsclean_stage = TableNames.menuitemsclean_pdf_stage
            # Table_menuitemsclean_new = TableNames.menuitemsclean_pdf_new
    
        # count items in menuitems_raw but not in menuitemsclean_stage
        #formerly count_html_documents
        # TODO: exclude the rows (in a new table) that contains only the idx of raw items that were previously attempted to be cleaned (failed+succeeded)
        self.count_raw_menuitems = f"""
            SELECT COUNT(distinct (u.country, r.idx, r.menuid, r.itemtext, r.item_extract_reihenfolge))
            FROM {TableNames.MENU_ITEM_RAW} r
            inner join {TableNames.MENU} m
            on r.idx = m.idx and r.menuid = m.menuid
            left join {table_b2b_urls} u on r.idx = u.idx
            WHERE m.quellenid = {{}} 
            AND NOT EXISTS (
                SELECT 1 from {TableNames.MENU_ITEM_CLEAN_STAGE} s 
                WHERE s.idx = r.idx and s.menuid = r.menuid
            )
            ;"""
    

    # formerly count_html_documents
    count_html_documents = f"""
            SELECT COUNT(*)
            FROM {TableNames.B2B_HTML} h
            INNER JOIN (select idx, max(version) "version" from {TableNames.B2B_HTML} group by idx) idxvers
                on h.idx = idxvers.idx and h.version = idxvers.version
            INNER JOIN {TableNames.B2B_URLS} u  
                ON u.idx = h.idx
            WHERE ( u.objektid IS NOT NULL)
                AND NOT EXISTS (
                    SELECT 1
                    FROM {TableNames.MENU_PROCESSED} p
                    WHERE u.idx = p.idx
                        AND h.version = p.version      
                )
            ;"""

    # formerly count_html_documents
    count_pdf_urls = f"""
            SELECT COUNT(*)
            FROM {TableNames.B2B_URLS_PDF} u
            JOIN (SELECT * from {TableNames.OBJEKT} WHERE quellenid != 45) o on u.objektid = o.objektid
            WHERE NOT EXISTS (SELECT 1 from {TableNames.MENU_PROCESSED} mp
            					where mp.idx = u.idx and 
            				(eingefuegtvon = 'me_pdfextractor' 
                            and date_trunc('day', eingefuegtam) > CURRENT_DATE - interval '1 months'))
            ;"""
    
    count_pdf_urls_dish = f"""
            SELECT COUNT(*)
            FROM {TableNames.B2B_URLS_PDF} u
            JOIN (SELECT * from {TableNames.OBJEKT} WHERE quellenid = 45) o on u.objektid = o.objektid
            WHERE NOT EXISTS (SELECT 1 from {TableNames.MENU_PROCESSED} mp
            					where mp.idx = u.idx and 
            				(eingefuegtvon = 'me_pdfextractor' 
                            and date_trunc('day', eingefuegtam) > CURRENT_DATE - interval '1 months'))
            ;"""

    search_item = f"""
        SELECT count(*)
        FROM {TableNames.MENU_ITEM_CLEAN}
        where idx = %s and itemname = %s and itemdescription = %s
        ;"""#.format(idx, itemname, itemdescription)


class DbFetchQueries:
    """Query string definitions for the queries
    that fetch data from the database"""
    def __init__(self, source_doctype='html'):
        if source_doctype == 'html':
            Table_menuitems_b2b = TableNames.B2B_URLS
            # Table_menu_items_raw = TableNames.menuitems_raw
            # Table_menuitemsclean_stage= TableNames.menuitemsclean_stage
            # Table_menuitemsclean_new= TableNames.menuitemsclean_new
        elif source_doctype in ['pdf', 'dishpdf']:
            Table_menuitems_b2b = TableNames.B2B_URLS_PDF
            # Table_menu_items_raw = TableNames.menuitems_raw_pdf
            # Table_menuitemsclean_stage = TableNames.menuitemsclean_pdf_stage
            # Table_menuitemsclean_new= TableNames.menuitemsclean_pdf_new
        
        #  TODO: exclude the rows (in a new table) that contains only the idx of raw items that were previously attempted to be cleaned (failed+succeeded)
        self.fetch_menu_items = f"""
            SELECT distinct u.country, r.idx, r.menuid, r.itemid,
                r.itemtext, r.item_extract_reihenfolge
            FROM {TableNames.MENU_ITEM_RAW} r 
            inner join {TableNames.MENU} m
            on r.idx = m.idx and r.menuid = m.menuid
            left join {Table_menuitems_b2b} u
            on r.idx = u.idx
            where m.quellenid = {{}}
            AND r.idx in (select distinct mir.idx from {TableNames.MENU_ITEM_RAW} mir 
                        left join 
                        {TableNames.MENU_ITEM_CLEAN_STAGE} mis on mir.idx = mis.idx
                        and mir.menuid = mis.menuid
                        where mis.idx is null
                        LIMIT {{}})
            -- ORDER BY r.idx ASC
            ;"""


    fetch_html_documents=f"""
            --- select the items based on the previous step, ignoring any other content
            --- multiple processes can be running at the same time. 
            SELECT h.idx,
                h.version,
                greatest(max(m.menuid),0) prev_menuid,
                u.url,
                u.url_absolute,
                h.html,
                u.objektid
                from {TableNames.B2B_HTML} h 
                inner join {TableNames.MENU_PROCESSED} mp
                on h.idx = mp.idx and h."version" = mp."version"
                left join {TableNames.MENU} m 
                on h.idx = m.idx
                inner join {TableNames.B2B_URLS} u 
                on h.idx = u.idx
                left join {TableNames.MENU_PROCESSED} mp2
                on h.idx = mp2.idx and h."version" = mp2."version" and mp2.eingefuegtvon != '{{}}'
                and mp2.eingefuegtvon like '{me_config.HTML_EXTRACTOR_STRING}-%'
                where mp.eingefuegtvon = '{{}}' -- select by the reservation of the current extractor
                and mp2.idx is null -- exclude the items that were reserved by another extractor
                group by h.idx, h.version, u.url, u.url_absolute, h.html, u.objektid
            """

        
    _fetch_html_documents_from_list = f"""
            SELECT h.idx,
                h.version,
                greatest(max(m.menuid),0) prev_menuid,
                u.url,
                u.url_absolute,
                h.html,
                u.objektid
            FROM {TableNames.B2B_HTML} h
            inner join (select idx, max(version) "version" from {TableNames.B2B_HTML} group by idx) idxvers
            	on h.idx = idxvers.idx and h.version = idxvers.version
            LEFT JOIN {TableNames.MENU} m
                on h.idx = m.idx
            INNER JOIN {TableNames.B2B_URLS} u
                ON u.idx = h.idx
            WHERE {{}} u.objektid IS NOT NULL
                AND NOT EXISTS (SELECT 1
                    FROM {TableNames.MENU_PROCESSED} p
                    WHERE u.idx = p.idx
                        AND h.version = p.version
                )
            GROUP BY h.idx, h.version, u.url, u.url_absolute, h.html, u.objektid
            ORDER BY h.idx DESC
                {{}}
                {{}} 
            ;"""
    
    # insert the small differences in the base query 
    # fetch_html_documents = _fetch_html_documents.format("", r"LIMIT  {}", r"OFFSET  {}")
    fetch_html_documents_from_list = _fetch_html_documents_from_list.format(r"ANY(%s) AND"+"\n                ", "", "")


    # formerly fetch_html_documents
    _fetch_pdf_urls = f"""
        With ranks as(
            SELECT lower(c.name) as country, r.id as country_priority
            FROM smartdatastagdb.config_country_export c
            join smartdatastagdb.config_dedup_order r on r.lkz = c.countrycode
        ),
        prep_query as (
            SELECT u.country, u.idx, u.url, u.url_absolute, u.objektid, greatest(max(m.menuid),0) prev_menuid, greatest(max(m.crawlversion), 1) prev_crawlversion, sh.sourcehash
            FROM {TableNames.B2B_URLS_PDF} u {{}}
            LEFT JOIN {TableNames.MENU} m
                on u.idx = m.idx
            LEFT JOIN 
            (
            	SELECT a.idx, max(a.sourcehash) sourcehash
				FROM {TableNames.MENU_PROCESSED} a
				JOIN (
				    SELECT idx, MAX(GREATEST(EINGEFUEGTAM, COALESCE(GEAENDERTAM, EINGEFUEGTAM))) AS MaxDate
				    FROM {TableNames.MENU_PROCESSED}
				    GROUP BY idx
				) b ON a.idx = b.idx AND GREATEST(a.EINGEFUEGTAM, COALESCE(a.GEAENDERTAM, a.EINGEFUEGTAM)) = b.MaxDate
				group by a.idx
			) sh
			on 
			u.idx = sh.idx
            WHERE {{}} NOT EXISTS (SELECT 1 from {TableNames.MENU_PROCESSED} mp
            					where mp.idx = u.idx and 
            				(eingefuegtvon = 'me_pdfextractor' 
                            and date_trunc('day', eingefuegtam) > CURRENT_DATE - interval '1 months'))
            GROUP BY u.country, u.idx, u.url, u.url_absolute, u.objektid, sh.sourcehash
        )
        SELECT idx, url, url_absolute, objektid, prev_menuid, prev_crawlversion, sourcehash
        FROM prep_query
        LEFT JOIN ranks
            on ranks.country = prep_query.country
        ORDER BY ranks.country_priority asc, idx desc
                {{}}
                {{}} 
            ;"""
    
    fetch_pdf_urls = _fetch_pdf_urls.format(
        f"\n            JOIN (SELECT * from {TableNames.OBJEKT} WHERE quellenid != 45) o on u.objektid = o.objektid",
        "", 
        r"LIMIT  {}", r"OFFSET  {}")

    fetch_pdf_urls_dish = _fetch_pdf_urls.format(
        f"\n            JOIN (SELECT * from {TableNames.OBJEKT} WHERE quellenid = 45) o on u.objektid = o.objektid",
        "", 
        r"LIMIT  {}", r"OFFSET  {}")
    
    fetch_pdf_urls_from_list = _fetch_pdf_urls.format(
        "",
        r"h.idx = ANY(%s) AND"+"\n                  ",
        "","")


class DbInsertQueries:
    """Query string definitions for the queries
    that insert data from the database. Single inserts
    are defined as strings, multi inserts as dicts. 
    The dicts contain the query string and the
    value template that is used to insert the data.
    Insert points use the "%s" placeholder for the
    values or value template to be inserted, respectively"""


    reserve_html_documents = f"""
            INSERT INTO {TableNames.MENU_PROCESSED} (
                idx, version, eingefuegtvon, eingefuegtam
            )
            SELECT 
                h.idx,
                h.version,
                '{{}}' AS eingefuegtvon, --insert the specific extractor id here
                now() AS eingefuegtam
            FROM smartdatadb.b2b_html h
            INNER JOIN (
                SELECT idx, max(version) AS version 
                FROM {TableNames.MENU_PROCESSED} 
                GROUP BY idx
            ) idxvers 
            ON h.idx = idxvers.idx AND h.version = idxvers.version
            INNER JOIN smartdatadb.b2b_urls u ON u.idx = h.idx
            WHERE u.objektid IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM {TableNames.MENU_PROCESSED} p
                WHERE u.idx = p.idx
                AND h.version = p.version
                AND p.eingefuegtvon like '{me_config.HTML_EXTRACTOR_STRING}%'
            )
            GROUP BY h.idx, h.version, u.url, u.url_absolute, h.html, u.objektid
            limit {{}}
            ;
            """

    insert_item_stage = f"""
            INSERT INTO {TableNames.MENU_ITEM_CLEAN_STAGE} (
            idx,
            menuid,
            itemid,
            itemname,
            itemprice,
            itemcurrency,
            itemsize,
            itemdescription,
            eingefuegtam,
            item_extract_reihenfolge
            
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s
        )   
        ;"""
    
    insert_item_stage_multi = {
            "query" : f"""INSERT INTO {TableNames.MENU_ITEM_CLEAN_STAGE} (
            idx,
            menuid,
            itemid,
            itemname,
            itemprice,
            itemcurrency,
            itemsize,
            itemdescription,
            eingefuegtam,
            item_extract_reihenfolge
            
            ) VALUES %s
            """,
            "value_template":"(%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)"
            }
    
    
    insert_item = f"""
            INSERT INTO {TableNames.MENU_ITEM_RAW} (
                idx, menuid, itemid, itemtext, eingefuegtam, eingefuegtvon, item_extract_reihenfolge
            ) VALUES (
                %s, %s, %s, %s, NOW(), %s, %s
            )
            ;"""
    
    insert_item_multi = {
            "query" : f"""INSERT INTO {TableNames.MENU_ITEM_RAW} (
                    idx, menuid, itemid, itemtext, eingefuegtam, eingefuegtvon, item_extract_reihenfolge) VALUES %s
                    ;""",
            "value_template":"(%s, %s, %s, %s, NOW(), %s, %s)"
            }
    
    #TODO: add the extractorid and quellenid dynamically (fetch with the htmls and pass through the pipeline)
    insert_menu_multi = { 
            "query" : f"""INSERT INTO {TableNames.MENU} (idx, menuid, extractorid, crawlversion, quellenid, eingefuegtvon) VALUES %s
                ;""",
            "value_template":"(%s, %s, %s, %s, %s, %s)"
    }

    insert_processed = f"""
            INSERT INTO {TableNames.MENU_PROCESSED} (
                idx, version, items, eingefuegtam, eingefuegtvon
            ) VALUES (
                %s, %s, %s, NOW(), 'html_extractor'
            )
            ;"""

    insert_processed_multi = {
            "query" : f"""INSERT INTO {TableNames.MENU_PROCESSED} (
                        idx, version, items, eingefuegtam, eingefuegtvon ) VALUES %s
                    ;""",
            "value_template": "(%s, %s, %s, NOW(), 'html_extractor')"
            }
    
    insert_processed_multi_pdf = {
            "query" : f"""INSERT INTO {TableNames.MENU_PROCESSED} (
                        idx, version, items, eingefuegtam, eingefuegtvon, next_process, sourcehash) VALUES %s
                    ;""",
            "value_template": "(%s, %s, %s, NOW(), 'me_pdfextractor', %s, %s)"
            }

    #formerly insert_item
    insert_cleaned_item = f"""
        INSERT INTO {TableNames.MENU_ITEM_CLEAN} (
            idx,
            menuid,
            itemid, 
            itemname,
            itemprice,
            itemcurrency,
            itemsize,
            itemdescription
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ;"""
    
    insert_cleaned_item_now = f"""
        INSERT INTO {TableNames.MENU_ITEM_CLEAN} (
            idx,
            menuid,
            itemid, 
            itemname,
            itemprice,
            itemcurrency,
            itemsize,
            itemdescription,
            eingefuegtam
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,%s, NOW()
        )
        ;"""

    insert_cleaned_item_now_multi = {
            "query": f"""
            with myvars (idx, menuid, itemid,  itemname, itemprice, itemcurrency, itemsize, itemdescription, eingefuegtam) 
                as 
                (values %s)

                insert into {TableNames.MENU_ITEM_CLEAN}
                        (select idx, itemname, itemprice, itemcurrency, itemsize, itemdescription, eingefuegtam from myvars
                    where not exists 
                        (select 1 from {TableNames.MENU_ITEM_CLEAN} mi, myvars where mi.idx = int4(myvars.idx) and mi.itemname = myvars.itemname and mi.itemdescription = myvars.itemdescription))
            """,
            "value_template":    "(%s, %s, %s, %s, %s, %s, %s, %s, NOW())"
    }
    
class DbUpdateQueries:
    """Query string definitions for the queries
    that update data from the database, or execute
    database functions."""
    def __init__(self, source_doctype='html'):

        if source_doctype == 'html':
            Function_insert_menuitems_from_stage = FunctionCalls.insert_menuitems_from_stage_html
            Table_menuitemsclean = TableNames.menuitemsclean
            Table_menuitemsclean_stage = TableNames.MENU_ITEM_CLEAN_STAGE
        elif source_doctype in ['pdf', 'dishpdf']:
            if source_doctype == 'pdf':
                Function_insert_menuitems_from_stage = FunctionCalls.insert_menuitems_from_stage_pdf
            else:
                Function_insert_menuitems_from_stage = FunctionCalls.insert_menuitems_from_stage_dishpdf
            Table_menuitemsclean = TableNames.menuitemsclean_pdf
            Table_menuitemsclean_stage = TableNames.menuitemsclean_pdf_stage

        self.submit_items_from_stage = f"call {Function_insert_menuitems_from_stage}"

        self.update_items_status =  f'''
            update {Table_menuitemsclean} 
            set status = False,
            geaendertvon= 'me_cleaner_v1_new_extraction_idx',
            geaendertam = now()
            where idx in (select idx from {Table_menuitemsclean_stage} group by idx)
            ;'''

    menuhash_and_remove_unchanged_menus = f"""
        begin; 
            /* Step 1: create a hash table for all fields of every item */
            /* Step 2: concatenate the items per menu*/
            /* Step 3: apply hash on the value for each idx, to deliver a single hash per idx */
            /* Step 4: insert the hash on every idx for the current cleaned set */

            with hashcalc as (
                select idx, menuid, md5(agg_hash) menu_hash 
                    from (
                        select idx, menuid, string_agg(md5(item), '') agg_hash
                        from (
                            select idx, menuid,
                            coalesce(itemname, '')||coalesce(itemprice::text, '')||coalesce(itemcurrency, '')||
                            coalesce(itemsize, '')||coalesce(itemdescription, '')||coalesce(flag_drink::text, '')||
                            coalesce(itemprice_numeric::text, '') as item
                            from {TableNames.MENU_ITEM_CLEAN_STAGE} mis 
                            ) as pre_agg
                        group by idx, menuid
                    ) aggregate_hash
                )
            update {TableNames.MENU} as m
                set menu_hash = hashcalc.menu_hash
                    from hashcalc
                where 
                    m.idx = hashcalc.idx and m.menuid = hashcalc.menuid
                ;
            /* Delete items that are already present with the same details in the clean items */
            delete from {TableNames.MENU_ITEM_CLEAN_STAGE} ms where exists
            (select 1 from {TableNames.MENU_ITEM_CLEAN} m where ms.idx = m.idx and ms.menuid = m.menuid and ms.itemid = m.itemid
                and ms.item_extract_reihenfolge = m.item_extract_reihenfolge and ms.itemname=m.itemname 
                and ms.itemprice=m.itemprice and ms.itemcurrency = m.itemcurrency and ms.itemdescription = m.itemdescription);


            /* Step 5: compare the current to the previous hash, if they exist. Where the hash is the same,
            * remove the current menu */
            create temp table no_new_menu on commit drop as 
                (
                select distinct s.idx, s.menuid from {TableNames.MENU_ITEM_CLEAN_STAGE} s
                    inner join 
                        {TableNames.MENU} m 
                    on s.idx = m.idx and s.menuid = m.menuid
                    inner join 
                        {TableNames.MENU} mp
                    on s.idx = mp.idx and s.menuid = mp.menuid+1
                    where m.menu_hash = mp.menu_hash
                );

            /* Step 6: add a new change date to the old menu in case the new extraction is the same */
            UPDATE {TableNames.MENU} m 
            SET geaendertam = CURRENT_DATE
            FROM no_new_menu nm
            WHERE nm.idx = m.idx AND nm.menuid - 1 = m.menuid;

            delete from {TableNames.MENU_ITEM_RAW} mir
            using no_new_menu nnm
            where mir.idx = nnm.idx and mir.menuid = nnm.menuid; 
	
            delete from {TableNames.MENU_ITEM_CLEAN_STAGE} mis
            using no_new_menu nnm
            where mis.idx = nnm.idx and mis.menuid = nnm.menuid;

            delete from {TableNames.MENU} m
            using no_new_menu nnm
            where m.idx = nnm.idx and m.menuid = nnm.menuid;

            /* Step 7: execute the insert operation into the clean items */
            insert into {TableNames.MENU_ITEM_CLEAN}
            select * from {TableNames.MENU_ITEM_CLEAN_STAGE} s where
            not exists (select 1 from {TableNames.MENU_ITEM_CLEAN} m where 
                s.idx = m.idx and s.menuid = m.menuid) ; --exclude apparent duplicates
            delete from {TableNames.MENU_ITEM_CLEAN_STAGE} where True;
            
        commit; 
    """

    update_menu_validity = f"""
            UPDATE {TableNames.MENU} AS m1
            SET gueltigbis = now()
            FROM (
                SELECT idx, MAX(menuid) AS max_menuid
                FROM {TableNames.MENU}
                GROUP BY idx
            ) AS m2
            WHERE m1.idx = m2.idx
            AND m1.menuid < m2.max_menuid
            AND (m1.gueltigbis IS NULL OR m1.gueltigbis > now());
        """ 
    
    update_menu_changed_date = f"""
        UPDATE {TableNames.MENU} m
        SET geaendertam = CURRENT_DATE,
            geaendertvon = '{{}}'
        WHERE m.idx = {{}}
        AND m.menuid = (
            SELECT MAX(mn.menuid)
            FROM {TableNames.MENU} mn
            WHERE mn.idx = m.idx
            );
        """
    

class DbDeleteQueries:
    """Query string definitions for the queries
    that delete data from the database."""
    def __init__(self, source_doctype='html'):
        if source_doctype == 'html':
            Table_menuitems_raw = TableNames.MENU_ITEM_RAW
            Table_menuitemsclean_stage = TableNames.MENU_ITEM_CLEAN_STAGE
            Table_menuitemsclean = TableNames.MENU_ITEM_CLEAN
        elif source_doctype in ['pdf', 'dishpdf']:
            Table_menuitems_raw = TableNames.MENU_ITEM_RAW
            Table_menuitemsclean_stage = TableNames.MENU_ITEM_CLEAN_STAGE
            Table_menuitemsclean = TableNames.MENU_ITEM_CLEAN
        
        self.delete_menu_items = f"""
        DELETE FROM {Table_menuitems_raw} r where TRUE;
        """
        
        # f"""
        #     WITH processed AS (
        #       SELECT idx,
        #         itemtext
        #       FROM {TableNames.menuitems_raw} r
        #       ORDER BY idx, itemtext
        #         LIMIT  10000
        #     )
        #     DELETE FROM {TableNames.menuitems_raw} r
        #     WHERE r.idx IN (SELECT idx FROM processed)
        #       AND r.itemtext IN (SELECT itemtext FROM processed)
        #     ;
        #     """
        
        self.remove_html_doc_reservations=f"""-- clean up the menuprocessed table after processing.
                        delete from {TableNames.MENU_PROCESSED} where eingefuegtvon = '{{}}' -- add the current extractor id here
                        -- delete the entries that the current extractor has finished.
                    """


        self.delete_items_from_stage = f"""
                delete from {Table_menuitemsclean_stage} s
                where exists (select 1 from {Table_menuitemsclean} n where s.pk_id =n.pk_id and n.status = True)
                ;
                """

        self.delete_no_items = f"""
                delete from {Table_menuitemsclean} 
                where lower(itemname) in (select lower(itemname) from {TableNames.MENU_ITEM_DEL})
                ;
                """

        self.delete_no_items_url = f"""
                delete from {Table_menuitemsclean}
                where idx in (select distinct idx from {TableNames.MENU_ITEM_DEL_URL}) 
                ;
                """

        self.delete_idx_items = f"""
                delete from {Table_menuitemsclean}
                where idx in (select idx
                from {Table_menuitemsclean} m 
                group by idx, eingefuegtam::date
                having count(itemname) < 2)
                ;
                """ 

    # TODO: decide wheter to use menuitemsclean or menuitemsraw as a fail for the menu.
    cleanup_raw_and_ids = f"""
            BEGIN;

            CREATE TEMP TABLE TEMP_IDX_SET AS 
            (SELECT IDX, MENUID FROM {TableNames.MENU_ITEM_CLEAN_STAGE}
            WHERE itemid = 0 and itemname = 'me_cleaner reserved');

            DELETE FROM {TableNames.MENU_ITEM_CLEAN_STAGE} 
                WHERE itemid = 0 and itemname = 'me_cleaner reserved'; 

            CREATE TEMP TABLE TEMP_ID_PAIRS AS 
            (SELECT DISTINCT mir.idx, mir.menuid FROM {TableNames.MENU_ITEM_RAW} mir
                LEFT JOIN {TableNames.MENU_ITEM_CLEAN_STAGE} mis
                    on mir.idx = mis.idx and mir.menuid = mis.menuid
                LEFT JOIN {TableNames.MENU_ITEM_CLEAN} mic
                    on mir.idx = mic.idx and mir.menuid = mic.menuid
                WHERE mis.idx is null and mic.idx is null)
                ;

            DELETE FROM {TableNames.MENU_ITEM_RAW} WHERE 
                (idx, menuid) IN (SELECT idx, menuid FROM TEMP_IDX_SET)
                ;

            DELETE FROM {TableNames.MENU} m
                USING TEMP_ID_PAIRS tip
                    WHERE tip.idx = m.idx and tip.menuid = m.menuid
                    ;

            DROP TABLE TEMP_IDX_SET; 

            DROP TABLE TEMP_ID_PAIRS; 

            COMMIT;
        """

    temp_clean_stage = f"""
    begin; 

    delete from {TableNames.MENU_ITEM_CLEAN_STAGE} where not exists
    (select 1 from {TableNames.MENU_PROCESSED} where idx = -999999998 and version = 0 and eingefuegtvon='me_cleaner_20241023');


    INSERT INTO {TableNames.MENU_PROCESSED} (idx, version, eingefuegtvon)
    select -999999998, 0, 'me_cleaner_20241023'
    where not exists
    (select 1 from {TableNames.MENU_PROCESSED} where idx = -999999998 and version = 0 and eingefuegtvon='me_cleaner_20241023');

    commit; 
    """ 