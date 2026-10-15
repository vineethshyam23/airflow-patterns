

SET search_path TO public;


do
$$
declare
	anzahl int;
	rec record;
    v_exec_sql text; 
begin
	anzahl :=0;
	delete from smartdatadb.tmp_b2b_keywords_match;
	for rec in (select string_agg(strcase, ' ') as strcase, string_agg(keywords, ',') as keywords from(
					select 'when keyword in ('||string_agg(''''||keyword||'''', ',')||' ) then '''||min(name) ||'''' as strcase,
					string_agg(''''||keyword||'''', ',') as keywords
					from (
						select 
						case 
							when name ~ 'or_[a-z]*|online_reservierung_[a-z]{0,2}' then 1
							when name ~ 'takeaway_[a-z]{0,2}' then 2
							when name ~ 'delivery[1|2]_[a-z]{0,2}' then 3
						end as grp,
						name, keyword from  smartdatadb.b2b_keywords k_1
						WHERE k_1.name ~ 'or_[a-z]*|online_reservierung_[a-z]{0,2}|takeaway_[a-z]{0,2}|delivery[1|2]_[a-z]{0,2}'
						)a
					group by grp)a) loop 
		 v_exec_sql := 'INSERT INTO smartdatadb.tmp_b2b_keywords_match (name, keyword, idx, version)
					select case '||rec.strcase||' end as name, km.keyword, km.idx, km.version from smartdatadb.b2b_keywords_match km 
					where km.keyword in ('||rec.keywords||')
					AND km.pos <> 0';
		raise notice '%: %', now(), v_exec_sql;	
		execute v_exec_sql;
		GET DIAGNOSTICS anzahl = ROW_COUNT;
		raise notice '%: Insert %', now(), anzahl;	
	end loop;
end;
$$ language plpgsql;  -- Added space before language for better readability

DO $$
Begin
REFRESH MATERIALIZED VIEW smartdatastagdb.mv_reservation_delivery_takeaway_base;
REFRESH MATERIALIZED VIEW smartdatastagdb.mv_reservation_delivery_takeaway;
call smartdatastagdb.refresh_masterobjekt_keyword_match();
end;
$$;  -- Added semicolon after the closing $$