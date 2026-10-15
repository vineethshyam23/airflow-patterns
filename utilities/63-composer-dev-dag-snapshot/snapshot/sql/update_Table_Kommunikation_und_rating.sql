--update Kommunikation
	
delete from smartdatadb.kommunikation
where text1 in ('','nicht vef�gbar','nicht verf�gabr','nicht verf�gbar','nicht vorhanden','Not Available','n. v.','n.v.') and text_log is null;
	
update smartdatadb.kommunikation
set text_log = text_log::_text|| text1::text,
	text1=null
where kommtypid = 3 and text1 !~* '(@|\[at\]|at|\(at\))';

--update ranting_yelp
	
delete from smartdatadb.objektmerkmal om352
where merkmalid = 352 and (case WHEN regexp_match(om352.wert::text, '[0-9][.,][[0-9]'::text) IS NOT NULL 
	  THEN btrim(replace(replace(om352.wert::text, ','::text, '.'::text), ' star rating'::text, ''::text))::numeric
	  ELSE NULL::numeric end ) not between 1 and 5;
    
--update rating_lieferando
	
delete from smartdatadb.objektmerkmal om372
where merkmalid = 372 and om372.wert::numeric not between 1 and 5;

--!!! neu 

--update Hausnur
insert into smartdatadb.adresse_history (adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon,history_eingefuegtam,history_eingefuegtvon)
select adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon, now(),'proc: update_Table_Kommunikation_und_rating'
from smartdatadb.adresse
where strasse  = hausnr  and strasse <>''
on conflict (adressid, history_eingefuegtam, history_eingefuegtvon)
do nothing;


update smartdatadb.adresse 
set hausnr = null  
where strasse  = hausnr  and strasse <>'';

--update Hausnur
/*
insert into smartdatadb.adresse_history (adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon,history_eingefuegtam,history_eingefuegtvon)
select adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon, now(),'Prozess: update_Table_Kommunikation_und_rating'
from smartdatadb.adresse a 
where hausnr ~* '^[?]|^["]{2}';

update smartdatadb.adresse 
set hausnr = null  
where hausnr ~* '^[?]|^["]{2}';
*/
--plz, lkz update

insert into smartdatadb.adresse_history (adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon,history_eingefuegtam,history_eingefuegtvon)
select adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon, now(),'proc: update_Table_Kommunikation_und_rating'
from smartdatadb.adresse a 
where plz in (select distinct plz from smartdatastagdb.matching_plz_lkz)
on conflict (adressid, history_eingefuegtam, history_eingefuegtvon)
do nothing;

update smartdatadb.adresse a
set lkz = (select distinct lkz from smartdatastagdb.matching_plz_lkz mpl where a.plz= mpl.plz and mpl.lkz is not null)
where plz in (select distinct plz from smartdatastagdb.matching_plz_lkz where lkz is not null);


update smartdatadb.adresse
set plz = null
where plz in (select distinct plz from smartdatastagdb.matching_plz_lkz);

insert into smartdatadb.adresse_history (adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon,history_eingefuegtam,history_eingefuegtvon)
select adressid, objektid, adresstypid, strasse, hausnr, postfach, lkz, plz, plzpostfach, ort, eingefuegtam,eingefuegtvon,geaendertam, geaendertvon, now(),'proc: update_Table_Kommunikation_und_rating'
from smartdatadb.adresse a 
where upper(plz)  = upper(ort) and plz !~* '[0-9]+' and plz <>''
on conflict (adressid, history_eingefuegtam, history_eingefuegtvon)
do nothing;

update smartdatadb.adresse
set plz = Null
where upper(plz)  = upper(ort) and plz !~* '[0-9]+' and plz <>'';

--update strasse 


update smartdatadb.adresse a 
set strasse = Null
where strasse !~* '[\w]+' and strasse <> '';



--update ort

/*update smartdatadb.adresse 
set ort = trim(ort,'-');
*/
--update tel

update smartdatadb.kommunikation
set text1 = null 
where kommtypid = 1 and text1 in ('None','Yes','2','1','-','<NA>','no','.','Anbieter möchte nicht veröffentlicht werden');

update smartdatadb.kommunikation
set text_log_tel = text_log_tel ::_text|| text1::text,
    text1 = null
where kommtypid = 1 and text1 in ('Gilgen''s','Locanda dei Sapori Perduti','4 AVENUE CRAMPEL','Plaza de Santa Maria 4');

call smartdatastagdb.update_kommunikation();
call smartdatastagdb.clean_masterobjekt_matching();







	
