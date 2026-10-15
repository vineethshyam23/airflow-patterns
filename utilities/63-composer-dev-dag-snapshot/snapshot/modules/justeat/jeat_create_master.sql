


-- DROP PROCEDURE smartdatadb.jeat_create_master(int4, int4);

CREATE OR REPLACE PROCEDURE smartdatadb.jeat_create_master(IN v_import_id integer, IN v_quellenid integer)
 LANGUAGE plpgsql
AS $procedure$
declare 
rec record;
v_objektid int; -- object id
v_commit_counter int := 0;
v_error_counter int := 0;
v_error_message text;
v_counter_insert int := 0;
v_counter_update int := 0;
v_error_flag char(10);
begin
    for rec in (select o.objektid, quellenid
				from smartdatadb.objekt o
				where o.masterobjektid is null -- master object id 
				and o.quellenid = v_quellenid -- source id
				and o.objektgruppeid <> 3 /* keine Master-Objekte -> no master objects */
				)
		loop 
		  begin
			v_commit_counter := v_commit_counter + 1;
		
			select nextval('smartdatadb.objekt_objektid_seq') into v_objektid;
		
			-- Masterobjekt anlegen / Create master object
            -- insert into smartdatadb.object (objectID, objectgroupID, sourceID, insertedon, insertedby)
			insert into smartdatadb.objekt (objektid, objektgruppeid, quellenid, eingefuegtam, eingefuegtvon)
			values (v_objektid, 3, rec.quellenid, now(), 'smartdatadb.jeat_create_master');
			
			v_counter_insert := v_counter_insert + 1;
		
			-- Master zuordnen -> assign master
			update smartdatadb.objekt 
			set masterobjektid = v_objektid,
			    geaendertam = now(), -- changed datetime
			    geaendertvon = 'smartdatadb.jeat_create_master' -- changed by
			where objektid = rec.objektid; -- object id
		
			v_counter_update := v_counter_update + 1;
			
		
			exception when others then 
				begin 
					v_error_message := SQLERRM;
				    v_error_counter := v_error_counter+1;
					insert into smartdatadb.import_bad_dwh (id, objektid, fehlermeldung, eingefuegtam)
					values (rec.objektid, v_objektid, v_error_message, now());
		 		end;
			
			end;
		
			if mod(v_commit_counter, 1000) = 0 then 
				commit;
		    end if; 
		end loop;
		
		select (case when v_error_counter = 0 then 'N' else 'J' end) into v_error_flag;
	
        -- smartdatastagdb.import_log (importid, process step, error flag, log_text, inserted_time)
		insert into smartdatastagdb.import_log (importid, prozessschritt, fehlerflag, log_text, eingefuegtam)
		values (v_import_id, 'Prozess: smartdatadb.jeat_create_master', v_error_flag, 'Insert: '|| v_counter_insert || ' Update: ' || v_counter_update || ' Error: ' ||v_error_counter, now());
	commit;
end;
$procedure$
;



