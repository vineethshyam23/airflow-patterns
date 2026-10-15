
do $$
declare 
	rec record;
	nUpdates int;
	nInserts int;
	i int;
	v_adressstatusid int;
	v_lastid bigint := 0;
	v_processed_count int := 0;
    
begin
	nUpdates := 0;
	nInserts := 0;
    -- Create tracking table if not exists
    CREATE TABLE IF NOT EXISTS smartdatastagdb.adresscheck_progress (
        last_adressid bigint DEFAULT 0
    );
    -- Initialize row if table empty
    IF NOT EXISTS (SELECT 1 FROM smartdatastagdb.adresscheck_progress) THEN
        INSERT INTO smartdatastagdb.adresscheck_progress (last_adressid) 
		(SELECT adressid FROM smartdatadb.adresse a 
		where not exists(select 1 from smartdatadb.adressstatushist a2 where a2.adressid=a.adressid and coalesce(a.geaendertam, a.eingefuegtam)<a2.gueltigab )
		order by a.adressid asc limit 1);
    END IF;
    -- Get last processed address ID
    SELECT max(last_adressid) INTO v_lastid FROM smartdatastagdb.adresscheck_progress LIMIT 1;

    RAISE NOTICE 'Last processed adressid: %', v_lastid;
    -- Process next 5000 addresses after last_adressid
	for rec in (select a.adressid, ac.*,
				md5(coalesce(a.lkz, '')||coalesce(a.plz, '')||coalesce(a.ort, '')||coalesce(a.strasse, '')||coalesce(a.hausnr, '')||coalesce(a.hausnrergaenzung,''))::uuid as adressehash
				from smartdatadb.adresse a, smartdatastagdb.AC_Addresscheck(a.lkz, a.plz, a.strasse, a.hausnr , a.hausnrergaenzung , a.ort) ac
				where not exists(select 1 from smartdatadb.adressstatushist a2 where a2.adressid=a.adressid 
				and coalesce(a.geaendertam, a.eingefuegtam)<a2.gueltigab )
				and a.lkz in (select lkz from smartdatastagdb.config_adresscheck_plz)
				and nullif(a.strasse,'') is not null and a.adressid > v_lastid
				Order by a.adressid ASC
				limit 10000) loop
		
		RAISE NOTICE 'Processing adressid: %', rec.adressid;
		begin
			-- Update address if data changed
			update smartdatadb.adresse a
			set lkz=rec.v_lkz,
				plz=rec.v_plz,
				ort=rec.v_ort,
				strasse=rec.v_strasse,
				hausnr=rec.v_hausnummer,
				hausnrergaenzung=rec.v_hausnummernzusatz,
				geaendertvon='adresscheck.sql'		
			where adressid=rec.adressid
			and coalesce(a.lkz,'')||'|'||coalesce(a.plz,'')||'|'||coalesce(a.ort,'')||'|'||coalesce(a.strasse,'')||'|'||coalesce(a.hausnr,'')||'|'||coalesce(a.hausnrergaenzung,'') <>
				coalesce(rec.v_lkz,'')||'|'||coalesce(rec.v_plz,'')||'|'||coalesce(rec.v_ort,'')||'|'||coalesce(rec.v_strasse,'')||'|'||coalesce(rec.v_hausnummer,'')||'|'||coalesce(rec.v_hausnummernzusatz,'')
			and nullif(rec.ac_status,'') is not null;
			GET DIAGNOSTICS i = ROW_COUNT;
			nUpdates := nUpdates+i;
			-- Determine latest status
			select first_value(ah.adressstatusid) over(partition by adressid order by gueltigab desc) 
			into v_adressstatusid
			from smartdatadb.adressstatushist ah 
			where adressid=rec.adressid;			
			-- Insert new status history if changed
			if  (v_adressstatusid=10 and rec.ac_status<>'korrekt-Treffer Str-HNR-PLZ-Ort') OR
				(v_adressstatusid=11 and rec.ac_status<>'korrekt-Treffer Str-PLZ-Ort') OR
				(v_adressstatusid=12 and rec.ac_status<>'korrekt-Treffer Str-PLZ') OR
				(v_adressstatusid= 1 and nullif(rec.ac_status,'') is null ) then
				INSERT INTO smartdatadb.adressstatushist (gueltigab, adressid, adressehash, adressstatusid) 
				VALUES(now(), rec.adressid, rec.adressehash,
					case when rec.ac_status='korrekt-Treffer Str-HNR-PLZ-Ort' then 10
						when rec.ac_status='korrekt-Treffer Str-PLZ-Ort' then 11
						when rec.ac_status='korrekt-Treffer Str-PLZ' then 12
						when nullif(rec.ac_status,'') is null then 1
					end );
				GET DIAGNOSTICS i = ROW_COUNT;
				nInserts := nInserts+i;
			end if;
			--Count as processed only after successful update/insert
			v_processed_count := v_processed_count + 1;
			RAISE NOTICE 'v_processed_count is: %',v_processed_count;
			--Update progress after each record
            UPDATE smartdatastagdb.adresscheck_progress
            SET last_adressid = rec.adressid;
			
		Exception 
           when others then raise notice 'Error checking address ID "%"', rec.adressid;   
        END;
		
	end loop;
	
	IF v_processed_count = 0 THEN
		IF v_lastid >= (SELECT max(adressid) FROM smartdatadb.adresse) THEN
			RAISE NOTICE 'No new records found — restarting from beginning.';
			UPDATE smartdatastagdb.adresscheck_progress
			SET last_adressid = 0;
		ELSE
			RAISE NOTICE 'No new records processed. Initializing from first valid id.';
			UPDATE smartdatastagdb.adresscheck_progress
			SET last_adressid = (SELECT adressid FROM smartdatadb.adresse a 
				where not exists(select 1 from smartdatadb.adressstatushist a2 where a2.adressid=a.adressid and coalesce(a.geaendertam, a.eingefuegtam)<a2.gueltigab )
				order by a.adressid asc limit 1);
		END IF;
	END IF;
    commit;
    raise notice '% Addresses updated, % address status history written.', nupdates, nInserts;    
end;
$$ language plpgsql



