"""
SQL builders for the absolute (multi-channel) monthly activity score DAG.

Each function returns a BigQuery Standard SQL string. Jinja macros such as
`{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}` are left
intact for Composer rendering.

Sanitized from dags/absolute_activityscores.py (read-only source).
"""

from __future__ import annotations


DEFAULT_PROJECT = "dwh_project"
REFINED = "refined"
TRUSTED = "trusted"
EXTERNAL = "external"
TRUSTED_VIEWS = "trusted_views"


def _fmt(sql: str, project: str = DEFAULT_PROJECT) -> str:
    return sql.format(project=project)

def query_as_absolute_base_wb_rt_event(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

     event_base as (

       select distinct 
            id_type, idchar, 
            date(derived_event_timestamp)as event_date, 
            case when id_type = 'trusted.rt_establishments.id' and regexp_contains(lower(event), 'notificationtarget') then 'RT notificationtarget change'
                  when id_type = 'trusted.hyd_establishments.id' and regexp_contains(lower(event), 'payment method') then 'Establishment payment methods changes'
                  when id_type = 'trusted.hyd_establishments.id' and regexp_contains(lower(event), 'menu') then 'Establishment menu changes'
                  when id_type = 'trusted.hyd_establishments.id' and regexp_contains(lower(event), 'stories') then 'Establishment stories changes'
                  else event 
            end as event,
            count(distinct idchar) over (partition by e.event, date(derived_event_timestamp)) as n_event_per_day 
        from `{project}.trusted.derived_events` e
        where id_type in ('trusted.hyd_establishments.id', 'trusted.rt_establishments.id')
            and _sourcesystem not like '%ExcludedRegion%'                
            and event not in ('Establishment modification date change',
                              'Establishment loc title change',
                              'Establishment loc description change')
                                  
            and (date_trunc(date(derived_event_timestamp), month) between '2019-09-01' and date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 month))                                
)

     select 
      * except (n_event_per_day)
     from event_base
     where n_event_per_day < 1000
    """,
        project,
    )

def query_as_absolute_base_rt_reservation(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """select 
          e.salesforce_id, 
          date_trunc(date(r.creation_date), month) as reservation_month,
          count(r.id_sk) as n_reservations, 
          max(date(r.creation_date)) date_last_activity_reservation
        from trusted_views.vrt_reservations r
        join refined.analytical_rt_establishments_actual e
          using (establishment_id_sk)
        where length(e.salesforce_id) > 0 
          and _valid_flag is true
          and status <> 'FREE' 
          and date_trunc(date(r.creation_date), month) <= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 month)                  
        group by 1,2""",
        project,
    )

def query_as_absolute_base_adobe(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with
    
product_login as (
      # CMS Website Login 

      select distinct 
        '',
        c.establishment_sfid,
        date(a.last_login_date) as login_date,
        "cms" as product_type

      from `{project}.trusted.hyd_users` a
      inner join `refined.analytical_sfdc_establishment_actual` b
        on a.salesforce_id = b.account_id
      inner join `refined.customer_base_establishment` c
        on b.establishment_id = c.establishment_sfid
      where c.Platform_CMS_createdDate is not null 
        and a.last_login_date is not null

      union all

      # Reservation Tool Login

      select distinct 
        '',
        c.establishment_sfid,
        date(a.last_login_date) as login_date,
        "reservation_tool" as product_type

      from `{project}.trusted.rt_users` a
      inner join `refined.analytical_sfdc_establishment_actual` b
        on a.salesforce_id = b.account_id
      inner join `refined.customer_base_establishment` c
        on b.establishment_id = c.establishment_sfid
      where c.Reservation_Tool_createdDate is not null
        and a.last_login_date is not null

      union all

      # Web Listing
      select distinct 
        '',
        ifnull(a.establishmentId, b.establishment_id) as establishmen_sfid, # Fill missing establishment_id based on account_id
        date(timestamp) as login_date,
        "web_listing" as product_type

      from `{project}.trusted_views.awl_event` a
      left join `refined.analytical_sfdc_establishment_actual` b
        on salesforceId = account_id
      inner join `refined.customer_base_establishment` c
      on ifnull(a.establishmentId, b.establishment_id) = establishment_sfid
      where Web_Listing_createdDate is not null

)
, adobe as (

      select distinct
        establishment_salesforce_id as salesforce_id, 
        if(lower(product_type) IN ('order_tool', 'order_tool_admin'), 'order_tool', product_type) as product_type,
        visit_date date,

        case 
          when product_type = 'cms' and visitor_type = 'end customer'
          then count(distinct visitor_id)
          else 0
        end as visitors,


        case 
          when visitor_type = 'restaurant owner'
          then count(distinct establishment_salesforce_id) 
          else 0
        end as login,
        
        #max(visit_date) over (partition by establishment_salesforce_id) date_last_activity_adobe

      from `{project}.refined.adobe_visit_visitor` a
      where visitor_type  in ('end customer', 'restaurant owner')
        and lower(product_type) in ('cms', 'web_listing', 'reservation_tool', 'order_tool', 'order_tool_admin')
        and establishment_salesforce_id is not null 
        and visit_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
      group by 1,2,3, visitor_type, a.product_type
)

, agg_login as (

     select distinct 
      ifnull(a.salesforce_id, b.establishment_sfid) as salesforce_id,
      ifnull(a.date, b.login_date) as date,           
      ifnull(a.product_type, b.product_type) as product_type, 
      1 as login

     from (
            select 
              * 
            from adobe 
            where login > 0
          ) a
     full join (
            select 
              * 
            from product_login
            where login_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          ) b
      on a.salesforce_id = b.establishment_sfid 
        and a.date = b.login_date
        and a.product_type = b.product_type
   
)

, cb as (

      select distinct 
        establishment_sfid, 
        date_add(date(Platform_CMS_createdDate), interval 7 day) as wb_creation_date,
        date_add(date(Reservation_Tool_createdDate), interval 7 day) as rt_creation_date,
        date_add(date(Web_Listing_createdDate), interval 7 day) as wl_creation_date,
        date_add(date(Order_Tool_createdDate), interval 7 day) as do_creation_date,
      from `{project}.refined.customer_base_establishment` 
)

, agg_visitor_login as (

      select  distinct 
        ifnull(a.salesforce_id, b.salesforce_id) salesforce_id,
        ifnull(a.date, b.date) as date,
        ifnull(a.product_type, b.product_type) product_type,
        sum(a.visitors) as visitors,
        ifnull(b.login, 0) as logins,
        max(ifnull(a.date, b.date) ) over (partition by ifnull(a.salesforce_id, b.salesforce_id)) date_last_activity_adobe

      from adobe a
      full join agg_login b
        on a.salesforce_id = b.salesforce_id
          and a.product_type = b.product_type
          and a.date = b.date
      group by 1,2,3,5, a.date, b.date, a.salesforce_id, b.salesforce_id

)

      select 
        cb.establishment_sfid, 
        adobe.product_type, 
        date_trunc(adobe.date, month) as month, 
        adobe.date_last_activity_adobe,
        sum(adobe.visitors) as visitor, 
        sum(adobe.logins) as login
      from cb
      join agg_visitor_login adobe
        on cb.establishment_sfid = adobe.salesforce_id 
      where (adobe.product_type = 'reservation_tool' and adobe.date >= cb.rt_creation_date)
        or  (adobe.product_type = 'cms' and adobe.date >= cb.wb_creation_date)
        or  (adobe.product_type = 'web_listing' and adobe.date >= cb.wl_creation_date)
        or  (adobe.product_type = 'order_tool' and adobe.date >= cb.do_creation_date)
      group by 1,2,3,4
      """,
        project,
    )

def query_as_absolute_base_adobe_mk(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

adobe as (
        select distinct   
          establishment_salesforce_id as salesforce_id, 
          branchoffice_id,
          date_trunc(visit_date, month) as hit_month,
          visit_date as date
        from  `{project}.refined.adobe_visit_visitor`
        where product_type in ('Menu_Kit') 
          and visitor_type in ('restaurant owner')
          and visit_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and visit_date >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year) 
          and  (establishment_salesforce_id is not null
              or branchoffice_id is not null)
)

,mk_base as (

        select 
          userHasEstablishmentId, 
          a.userID, 
          platform_customer_id, 
          mke.establishmentId, 
          platform_establishment_id, 
          count(platform_establishment_id) over (partition by c.platform_customer_id ) as n_est, 
        from `{project}.trusted_views.amenu_kit_user_has_establishment` a
        right join `trusted_views.amenu_kit_establishment` mke
        using (establishmentID)
        left join (
                    select 
                      * 
                    from trusted_views.amenu_kit_user 
                    where platform_customer_id is not null 
                      and platform_customer_id != '' 
                      and not regexp_contains(lower(platform_customer_id), 'manually')
                    ) c
          on a.userID = c.userID
)

, mk as (
        select 
          userID, 
          platform_customer_id account_sfid, 
          establishmentId branch_office_id,                      
          ifnull(platform_establishment_id, if(n_est = 0, e.UID__c, null)) establishment_sfid

        from mk_base mk
        left join `trusted_views.asfdc_account` acc
          on platform_customer_id = UID__c 
        left join (
                    select * 
                    from `trusted_views.asfdc_asset` 
                    where status = "Active"
                  ) a 
          on AccountId = acc.id
        left join `trusted_views.asfdc_establishment` e 
          on a.Establishment__c = e.id

)

, mk_user_has_login as (

        select distinct 
          userId, 
          date(trackTime) login_date
        from `{project}.trusted_views.vmenu_kit_tracking` a
        join `{project}.trusted_views.amenu_kit_trackingtype` b
          on a.trackingTypeId = b.trackingTypeId 
            and b.trackType = 'login' #and date(trackTime) >= date_sub(date_trunc(current_date, month), interval 12 MonTH)
)

, mk_product_login as (

        select distinct 
          cast (mk.branch_office_id as string) branch_office_id, 
          mk.establishment_sfid, 
          mk_user_has_login.login_date,
          'menu_kit' as product_type
        from mk
        join mk_user_has_login
          on mk.userId = mk_user_has_login.userId

)


        select distinct 
          ifnull(a.salesforce_id, b.establishment_sfid) as salesforce_id,
          ifnull (a.branchoffice_id, b.branch_office_id) as branchoffice_id,
          date_trunc(ifnull(a.date, b.login_date), month) as hit_month,
          ifnull(a.date, b.login_date) as date,           
          1 as login_mk,
          max(ifnull(a.date, b.login_date)) over (partition by ifnull(a.salesforce_id, b.establishment_sfid), 
                                                               ifnull (a.branchoffice_id, b.branch_office_id)) as last_date_mk_login
              
        from adobe a
        full join (
                  select 
                    * 
                  from mk_product_login
                  where login_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
                    and login_date >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year)  
                  ) b
          on (a.branchoffice_id = cast(b.branch_office_id as string)  
              or a.branchoffice_id = b.establishment_sfid
              or a.salesforce_id = b.establishment_sfid) 
            and a.date = b.login_date""",
        project,
    )

def query_as_absolute_wb_login_visitor(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with


est as (  

      select 
        c.establishment_sfid, 
        date(Platform_CMS_createdDate) org_creation_date, 
        date(Platform_CMS_deletedDate) Platform_CMS_deletedDate,
        iso_code as country_iso, 
        # customer lifetime = deletion date or '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' - creation date (if deletion date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' then take deletion date)
        case 
          when date_diff(
                            ifnull(if(date(Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                            date_add(date(Platform_CMS_createdDate), interval 7 day), month) >= 12 
            then 12
          when date_diff(
                            ifnull(if(date(Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                            date_add(date(Platform_CMS_createdDate), interval 7 day), month) = 0 
            then 1
          else date_diff(
                            ifnull(if(date(Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                            date_add(date(Platform_CMS_createdDate), interval 7 day), month)
        end as n_12_month,

        case 
          when date_diff(
                            ifnull(if(date(Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                            date_add(date(Platform_CMS_createdDate), interval 7 day), month) >= 6 
            then 6
          when date_diff(
                            ifnull(if(date(Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                            date_add(date(Platform_CMS_createdDate), interval 7 day), month) = 0 
            then 1
          else date_diff(
                            ifnull(if(date(Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                            date_add(date(Platform_CMS_createdDate), interval 7 day), month)
        end as n_6_month,   
        
      from `refined.customer_base_establishment`  c 
      left join trusted_views.ahyd_establishments a
      on c.establishment_sfid = a.salesforce_id
      left join `trusted_views.ahyd_countries` cn
      on a.country_id_sk = cn.id_sk
      where c.Platform_CMS_createdDate is not null 
        and (date(Platform_CMS_deletedDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or Platform_CMS_deletedDate is null)  # take all establishments that still exist until '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
        and date(Platform_CMS_createdDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' # take establishments created before '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
      group by 1,2,3,4,5,6
)


, adobe  as (

      select 
        * 
      from `{project}.refined.as_absolute_base_adobe` 
      where product_type = 'cms'
        and month < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
        and month >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year) 
)


, visitor_login as (

      select 
        e.establishment_sfid as salesforce_id, 
        e.country_iso, 
        a.month as hit_month, 
        e.n_6_month, 
        e.n_12_month,
        if(a.month is not null, sum(a.visitor), 0) as visitors,
        if(a.month is not null, sum(a.login), 0) as logins,

      from est e
      left join adobe a
      on e.establishment_sfid = a.establishment_sfid
      group by 1,2,3,4,5
)


, monthly_visitor_login_last_1y as (

      select 
        salesforce_id, 
        country_iso,
        round(sum(visitors)/n_12_month,2) monthly_visitors_last_1y,
        round(sum(logins)/n_12_month,2) monthly_login_last_1y
            
      from visitor_login
      where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -12 month) 
        or hit_month is null
      group by 1, 2, n_12_month

),


monthly_visitor_login_last_6m as (  

      select 
        salesforce_id, 
        country_iso,
        round(sum(visitors)/n_6_month,2) monthly_visitors_last_6m,
        round(sum(logins)/n_6_month,2) monthly_login_last_6m
            
      from visitor_login
      where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month) 
        or hit_month is null
      group by 1, 2, n_6_month

),

monthly_visitor_login_last_1m as (  

      select distinct
        salesforce_id, 
        country_iso, 
        visitors monthly_visitors_last_1m, 
        logins monthly_login_last_1m
  
      from visitor_login
      where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month) 
        or hit_month is null

),


-- weighted aggregated monthly visitor/login: aggregated = 0.5*last_1_month + 0.3*last_6_months + 0.2*last_1_year. 

establishment_visitor_login as ( 
      
      select 
        salesforce_id, 
        coalesce(a.country_iso, b.country_iso, c.country_iso) country_code,
        a.monthly_visitors_last_1y, 
        ifnull(b.monthly_visitors_last_6m, 0) monthly_visitors_last_6m , 
        ifnull(c.monthly_visitors_last_1m, 0) monthly_visitors_last_1m,

        round(0.5*ifnull(c.monthly_visitors_last_1m, 0) + 
              0.3*ifnull(b.monthly_visitors_last_6m, 0) + 
              0.2*a.monthly_visitors_last_1y, 2) as agg_visitor,
              
        a.monthly_login_last_1y, 
        ifnull(b.monthly_login_last_6m, 0) monthly_login_last_6m , 
        ifnull(c.monthly_login_last_1m, 0) monthly_login_last_1m,

        round(0.5*ifnull(c.monthly_login_last_1m, 0) + 
              0.3*ifnull(b.monthly_login_last_6m, 0) + 
              0.2*a.monthly_login_last_1y, 2) as agg_login_rel, # agg_login for relative AS, will be set as agg_login if the threshold for Website login > 1 yearly

        monthly_login_last_1y as agg_login # for absolute AS if the threshold for Website login = 1 yearly
                    
      from monthly_visitor_login_last_1y a
      full outer join monthly_visitor_login_last_6m b using (salesforce_id)
      full outer join monthly_visitor_login_last_1m c using (salesforce_id)

        
)  


, stat_visitor as (

      select distinct 
        country_code, 
        round(avg(agg_visitor) over (partition by country_code), 5) as mean_visitor_country,
        round(stddev(agg_visitor) over (partition by country_code), 5) as sd_visitor_country,    
        round(percentile_cont(agg_visitor, 0.5) over (partition by country_code), 5) as median_visitor_country

      from establishment_visitor_login
      where agg_visitor != 0 
      
),


stat_login as (

      select distinct 
        country_code, 
        round(avg(agg_login_rel) over (partition by country_code),5) as mean_login_country,
        round(stddev(agg_login_rel) over (partition by country_code),5) as sd_login_country,   
        round(percentile_cont(agg_login_rel, 0.5) over (partition by country_code),5) as median_login_country

      from establishment_visitor_login
      where agg_login_rel != 0

)


      select distinct

        a.salesforce_id, 
        a.country_code, 
        a.agg_visitor as agg_visitors, 
        a.agg_login, 
        a.monthly_login_last_1y, 
        a.monthly_login_last_6m , 
        a.monthly_login_last_1m,
        a.monthly_visitors_last_1y, 
        a.monthly_visitors_last_6m, 
        a.monthly_visitors_last_1m,
        ifnull(b.mean_visitor_country,0) as mean_visitors_country, 
        ifnull(c.mean_login_country,0) as mean_login_country, 
        ifnull(b.sd_visitor_country,0) as sd_visitors_country , 
        ifnull(c.sd_login_country,0) as sd_login_country, 
        ifnull(b.median_visitor_country,0) as median_visitors_country, 
        ifnull(c.median_login_country,0) as median_login_country, 

        case when a.agg_visitor = 0 then 0
            when a.agg_visitor < b.mean_visitor_country then 1
            when agg_visitor between b.mean_visitor_country and (mean_visitor_country + sd_visitor_country) then 2
            else 3
        end as country_WB_visitors_score_rel,
        
      
        case when agg_login_rel = 0 then 0
            when agg_login_rel < mean_login_country then 1
            when agg_login_rel between mean_login_country and (mean_login_country + sd_login_country) then 2
            else 3
        end as country_WB_login_score_rel,    

        date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month
          
      from establishment_visitor_login a
      left join stat_visitor b using (country_code)
      left join stat_login c using (country_code)""",
        project,
    )

def query_as_absolute_wb_event(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

est as (  

        select distinct 
          c.establishment_sfid as salesforce_id, 
          a.id_sk establishment_id_sk, 
          date(c.Platform_CMS_createdDate) org_creation_date, 
          date(c.Platform_CMS_deletedDate) Platform_CMS_deletedDate,
          country as country_code, 
          case when date_diff(
                              ifnull(if(date(c.Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(c.Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                              date_add(date(c.Platform_CMS_createdDate), interval 7 day), month) >= 12 
              then 12
              when date_diff(
                              ifnull(if(date(c.Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(c.Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                              date_add(date(c.Platform_CMS_createdDate), interval 7 day), month) = 0 
              then 1
              else date_diff(
                              ifnull(if(date(c.Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(c.Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                              date_add(date(c.Platform_CMS_createdDate), interval 7 day), month)
          end as n_12_month,

          case when date_diff(
                              ifnull(if(date(c.Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(c.Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                              date_add(date(c.Platform_CMS_createdDate), interval 7 day), month) >= 6 
              then 6
              when date_diff(
                              ifnull(if(date(c.Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(c.Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                              date_add(date(c.Platform_CMS_createdDate), interval 7 day), month) = 0 
              then 1
              else date_diff(
                              ifnull(if(date(c.Platform_CMS_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(c.Platform_CMS_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), 
                              date_add(date(c.Platform_CMS_createdDate), interval 7 day), month)
          end as n_6_month,   

          date_add(date(Platform_CMS_createdDate), interval 7 day) creation_date
               
        from `refined.customer_base_establishment` c 
        left join trusted_views.ahyd_establishments a
        on c.establishment_sfid = a.salesforce_id
        where c.Platform_CMS_createdDate is not null 
          and (date(c.Platform_CMS_deletedDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
                or c.Platform_CMS_deletedDate is null)  # take all establishments that still exist until '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and date(c.Platform_CMS_createdDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' # take establishments created before '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
)


, event as (

        select *, 
          date_trunc(event_date, month) as event_month, 
          max(event_date) over (partition by idchar) date_last_activity_wb_event
        from refined.as_absolute_base_wb_rt_event
        where id_type = 'trusted.hyd_establishments.id' 
)

, event_est as (
  
        select distinct 
          ev.idchar, 
          ev.event_month, 
          ev.event_date, 
          ev.event

        from est e
        join event ev on e.establishment_id_sk = ev.idchar
        where ev.event_date > e.creation_date
          and ev.event_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and ev.event_date >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year)

)


, event_actual as (      
  
        select distinct 
          e.salesforce_id, 
          e.country_code, 
          ee.event_month, 
          e.n_12_month, 
          e.n_6_month,
          case when ee.event_month is not null then count(ee.event) over (partition by ee.event_month, e.salesforce_id)
                else 0 
          end as n_event_month,
          max(ev.date_last_activity_wb_event) over (partition by e.salesforce_id) date_last_activity_wb_event
               
        from est e
        left join event_est ee 
          on e.establishment_id_sk = ee.idchar 
        left join (
                    select distinct 
                      idchar, 
                      date_last_activity_wb_event 
                    from event
                  ) ev 
          on e.establishment_id_sk = ev.idchar 
       
)


, monthly_event_last_1y as (

      select 
        salesforce_id, 
        country_code, 
        date_last_activity_wb_event, 
        round(sum(n_event_month)/n_12_month,2) as monthly_event_last_1y, 
            
      from event_actual
      where event_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -12 month) 
        or event_month is null
      group by 1,2,3, n_12_month
  
)

,  monthly_event_last_6m as (  
  
      select 
        salesforce_id, 
        country_code, 
        round(sum(n_event_month)/n_6_month,2) as monthly_event_last_6m
            
      from event_actual
      where event_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month) 
        or event_month is null
      group by 1, 2, n_6_month
  
)

, monthly_event_last_1m as ( 

      select 
        salesforce_id, 
        country_code, 
        sum(n_event_month) as monthly_event_last_1m
  
      from event_actual
      where event_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month) 
        or event_month is null
      group by 1,2
)


, establishment_event as ( 

        select 
          salesforce_id, 
          coalesce(a.country_code, b.country_code, c.country_code) as country_code,
          date_last_activity_wb_event,
          ifnull(monthly_event_last_1m, 0) monthly_event_last_1m,
          ifnull(monthly_event_last_6m, 0) monthly_event_last_6m,
          monthly_event_last_1y,
          round(0.5*ifnull(monthly_event_last_1m, 0) + 
                0.3*ifnull(monthly_event_last_6m, 0) + 
                0.2*monthly_event_last_1y, 2) agg_event_rel,
          monthly_event_last_1y as agg_event,
          'a' as t 
        from monthly_event_last_1y a
        full outer join monthly_event_last_6m b using (salesforce_id)
        full outer join monthly_event_last_1m c using (salesforce_id)
        
)    


, stat_event as (

        select distinct 
          country_code, 
          round(avg(agg_event_rel) over (partition by country_code),5) as mean_event_country,
          round(stddev(agg_event_rel) over (partition by country_code),5) as sd_event_country,    
          round(percentile_cont(agg_event_rel, 0.5) over (partition by country_code),5) as median_event_country,

        from establishment_event
        where agg_event != 0 
)

, int_stat_event as (

        select distinct t, 
            round(avg(agg_event_rel) over (partition by t),5) as mean_event_total,
            round(stddev(agg_event_rel) over (partition by t),5) as sd_event_total,    
            round(percentile_cont(agg_event_rel, 0.5) over (partition by t),5) as median_event_total,

        from establishment_event
        where agg_event != 0 
)


        select distinct 
          a.salesforce_id, 
          a.country_code,
          a.monthly_event_last_1y, 
          a.monthly_event_last_6m , 
          a.monthly_event_last_1m,
          a.agg_event, 
          ifnull(b.mean_event_country,0) mean_event_country, 
          ifnull(b.sd_event_country,0) sd_event_country, 
          ifnull(b.median_event_country,0) median_event_country,

          case when a.agg_event_rel = 0 then 0
              when a.agg_event_rel < b.mean_event_country then 1
              when a.agg_event_rel between b.mean_event_country and (b.mean_event_country + b.sd_event_country) then 2
              else 3
          end as country_WB_event_score_rel,

          c.mean_event_total, c.sd_event_total, c.median_event_total,
          
          case when a.agg_event_rel = 0 then 0
              when a.agg_event_rel < c.mean_event_total then 1
              when a.agg_event_rel between c.mean_event_total and (c.mean_event_total + c.sd_event_total) then 2
              else 3
          end as int_WB_event_score_rel,
              
          date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month,
          a.date_last_activity_wb_event

        from establishment_event a
        left join stat_event b using (country_code)
        left join int_stat_event c  using (t)
    
    """,
        project,
    )

def query_as_absolute_rt_reservation(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

est as (
        select distinct 
          a.salesforce_id, 
          date(a.creation_date) org_creation_date, 
          a.deletion_date_dt,
          country_code, 
          case when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) >= 12 then 12
              when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) = 0 then 1
              else date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month)
          end as n_12_month,

          case when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) >= 6 then 6
              when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) = 0 then 1
              else date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month)
          end as n_6_month,   
             
               
        from refined.analytical_rt_establishments_actual a
        join `refined.customer_base_establishment` c on c.establishment_sfid = a.salesforce_id
        where c.Reservation_Tool_createdDate is not null 
              and (a.deletion_date_dt >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or a.deletion_date_dt is null)
              and date(a.creation_date) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
)

, reservation as (

        select *
        from refined.as_absolute_base_rt_reservation
        where reservation_month < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and reservation_month >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year)
      
)

, last_date as (

        select 
          salesforce_id, 
          max(date_last_activity_reservation) as date_last_activity_reservation
        from refined.as_absolute_base_rt_reservation
        group by 1
)

, reservation_actual as (

        select 
          e.salesforce_id, 
          country_code, 
          r.reservation_month, 
          e.n_12_month, 
          e.n_6_month, 
          if(reservation_month is null, 0, sum(r.n_reservations)) as n_reservations

        from  est e
        left join reservation r
        using (salesforce_id)
        group by 1,2,3,4,5
)


, monthly_res_last_1y as (
     
        select 
          salesforce_id,  
          country_code, 
          round(sum(n_reservations)/n_12_month,2) as monthly_reservation_last_1y

        from reservation_actual
        where reservation_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -12 month) 
          or reservation_month is null
        group by 1, 2, n_12_month

),

monthly_res_last_6m as (  

        select 
          salesforce_id,  
          country_code, 
          round(sum(n_reservations)/n_6_month,2) as monthly_reservation_last_6m 

        from reservation_actual
        where reservation_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month) or reservation_month is null
        group by 1,  country_code, n_6_month

),

monthly_res_last_1m as (  

        select 
          salesforce_id,  
          country_code, 
          n_reservations as monthly_reservation_last_1m 

        from reservation_actual
        where reservation_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month) 
          or reservation_month is null
        group by 1,2,3

),
      
establishment_reservation as (

        select 
          salesforce_id, 
          coalesce(a.country_code, b.country_code, c.country_code) country_code,
          monthly_reservation_last_1y, 
          ifnull(monthly_reservation_last_6m, 0) as monthly_reservation_last_6m , 
          ifnull(monthly_reservation_last_1m, 0) as monthly_reservation_last_1m,
          round(0.5*ifnull(monthly_reservation_last_1m, 0) + 
                0.3*ifnull(monthly_reservation_last_6m, 0) + 
                0.2*monthly_reservation_last_1y,2) as agg_reservations,
          'a' as t
        from monthly_res_last_1y a
        full outer join monthly_res_last_6m b using (salesforce_id)
        full outer join monthly_res_last_1m c using (salesforce_id)

      
)


, stat_reservation as (

        select distinct 
          country_code, 
          round(avg(agg_reservations) over (partition by country_code),5) as mean_reservation_country,
          round(stddev(agg_reservations) over (partition by country_code),5) as sd_reservation_country,
          round(percentile_cont(agg_reservations, 0.5) over (partition by country_code),5) as median_reservation_country,

        from establishment_reservation
        where agg_reservations != 0 

)

, int_stat_reservation as (

        select distinct 
            t, 
            round(avg(agg_reservations) over (partition by t),5) as int_mean_reservation,
            round(stddev(agg_reservations) over (partition by t),5) as int_sd_reservation,
            round(percentile_cont(agg_reservations, 0.5) over (partition by t),5) as int_median_reservation,

        from establishment_reservation
        where agg_reservations != 0 

)


        select distinct 
          a.salesforce_id , 
          a.country_code, 
          a.monthly_reservation_last_1y, 
          a.monthly_reservation_last_6m , 
          a.monthly_reservation_last_1m, 
          a.agg_reservations, 
          ifnull(b.mean_reservation_country, 0) mean_reservation_country, 
          ifnull(b.sd_reservation_country, 0) sd_reservation_country,  
          ifnull(b.median_reservation_country, 0) median_reservation_country,
          c.int_mean_reservation, c.int_sd_reservation, c.int_median_reservation,    
          case when a.agg_reservations = 0 then 0
              when a.agg_reservations < b.mean_reservation_country then 1
              when a.agg_reservations between b.mean_reservation_country and (b.mean_reservation_country + b.sd_reservation_country) then 2
              else 3
          end as country_RT_reservation_score_rel,

          case when a.agg_reservations = 0 then 0
              when a.agg_reservations < c.int_mean_reservation then 1
              when a.agg_reservations between c.int_mean_reservation and (c.int_mean_reservation + c.int_sd_reservation) then 2
              else 3
          end as int_RT_reservation_score_rel,          
          
          date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') AS as_month,
          d.date_last_activity_reservation
        
        from establishment_reservation a
        left join stat_reservation b using (country_code)
        left join int_stat_reservation c using (t)
        left join last_date d using (salesforce_id)
    
    """,
        project,
    )

def query_as_absolute_rt_event(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

est as (
        select distinct 
          a.establishment_id_sk, 
          a.salesforce_id, 
          date(a.creation_date) org_creation_date, 
          a.deletion_date_dt,
          a.country_code, 
          case 
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) >= 12 then 12
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) = 0 then 1
            else date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month)
          end as n_12_month,

          case 
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) >= 6 then 6
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) = 0 then 1
            else date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month)
          end as n_6_month,           
          
          date_add(a.creation_date_dt, interval 7 day) as creation_date
               
        from refined.analytical_rt_establishments_actual a
        join `refined.customer_base_establishment`  c 
          on c.establishment_sfid = a.salesforce_id
        where c.Reservation_Tool_createdDate is not null 
          and (a.deletion_date_dt >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or a.deletion_date_dt is null)
          and date(a.creation_date) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
)


, event as (

        select 
          *, 
          date_trunc(event_date, month) as event_month,
          max(event_date) over (partition by idchar) date_last_activity_rt_event

        from refined.as_absolute_base_wb_rt_event
        where id_type = 'trusted.rt_establishments.id'

)


, event_est as (
  
        select distinct 
          ev.idchar, 
          ev.event_month, 
          ev.event_date, 
          ev.event

        from est e
        join event ev 
          on e.establishment_id_sk = ev.idchar
        where ev.event_date > e.creation_date
          and event_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and event_date >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year)

)


, event_actual as (      
  
        select distinct 
          salesforce_id, 
          country_code, 
          ee.event_month, 
          n_12_month, 
          n_6_month,
          case 
            when ee.event_month is not null then count(ee.event) over (partition by ee.event_month, salesforce_id)
            else 0 
          end as n_event_month,
          max(date_last_activity_rt_event) over (partition by salesforce_id) as date_last_activity_rt_event
               
        from est e
        left join event_est ee 
          on e.establishment_id_sk = ee.idchar 
        left join (
                    select distinct 
                      idchar, 
                      date_last_activity_rt_event 
                    from event
                  ) ev 
          on e.establishment_id_sk = ev.idchar 
)



, monthly_event_last_1y as (

        select 
          salesforce_id, 
          country_code, 
          date_last_activity_rt_event, 
          round(sum(n_event_month)/n_12_month,2) as monthly_event_last_1y, 
              
        from event_actual
        where event_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -12 month) 
          or event_month is null
        group by 1,2,3, n_12_month, country_code
  
)


,  monthly_event_last_6m as (  
  
        select 
          salesforce_id, 
          country_code, 
          round(sum(n_event_month)/n_6_month,2) as monthly_event_last_6m
              
        from event_actual
        where event_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month) 
          or event_month is null
        group by 1, 2, n_6_month
  
)


, monthly_event_last_1m as (

        select distinct 
          salesforce_id, 
          country_code, 
          n_event_month as monthly_event_last_1m
    
        from event_actual
        where event_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month) 
          or event_month is null
)


, establishment_event as ( 

        select distinct 
          a.salesforce_id, 
          coalesce(a.country_code, b.country_code, c.country_code) as country_code,
          a.date_last_activity_rt_event,
          ifnull(c.monthly_event_last_1m, 0) as monthly_event_last_1m,
          ifnull(b.monthly_event_last_6m, 0) as monthly_event_last_6m,
          a.monthly_event_last_1y,
          round(0.5*ifnull(c.monthly_event_last_1m, 0) + 
                0.3*ifnull(b.monthly_event_last_6m, 0) + 
                0.2*a.monthly_event_last_1y,2) as agg_event,
          'a' as t 

        from monthly_event_last_1y a
        full outer join monthly_event_last_6m b using (salesforce_id)
        full outer join monthly_event_last_1m c using (salesforce_id)
        
)    


, stat_event as (

        select distinct 
          country_code, 
          round(avg(agg_event) over (partition by country_code),5) as mean_event_country,
          round(stddev(agg_event) over (partition by country_code),5) as sd_event_country,    
          round(percentile_cont(agg_event, 0.5) over (partition by country_code),5) as median_event_country,

        from establishment_event
        where agg_event != 0 
)

, int_stat_event as (

      select distinct 
        t, 
        round(avg(agg_event) over (partition by t),5) as int_mean_event,
        round(stddev(agg_event) over (partition by t),5) as int_sd_event,    
        round(percentile_cont(agg_event, 0.5) over (partition by t),5) as int_median_event,

      from establishment_event
      where agg_event != 0 
)


      select distinct 
        a.salesforce_id, 
        a.country_code, 
        a.monthly_event_last_1m,
        a.monthly_event_last_6m,
        a.monthly_event_last_1y,
        a.agg_event, 
        ifnull(c.mean_event_country, 0) as mean_event_country, 
        ifnull(c.sd_event_country, 0) as sd_event_country, 
        ifnull(c.median_event_country, 0) as median_event_country,

        case 
          when a.agg_event = 0 then 0
          when a.agg_event < c.mean_event_country then 1
          when a.agg_event between c.mean_event_country and (c.mean_event_country + c.sd_event_country) then 2
          else 3
        end as country_RT_event_score_rel,

        int.int_mean_event, 
        int.int_sd_event, 
        int.int_median_event,

        case 
          when a.agg_event = 0 then 0
          when a.agg_event < int.int_mean_event then 1
          when a.agg_event between int.int_mean_event and (int.int_mean_event + int.int_sd_event) then 2
          else 3
        end as int_RT_event_score_rel,

        date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month,
        a.date_last_activity_rt_event

      from establishment_event a
      left join stat_event c using (country_code)
      left join int_stat_event int using (t)
    
    """,
        project,
    )

def query_as_absolute_rt_login(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with 
est as (
        select distinct 
          a.establishment_id_sk, 
          a.salesforce_id, 
          date(a.creation_date) org_creation_date, 
          a.deletion_date_dt,
          a.country_code, 
          case 
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) >= 12 then 12
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) = 0 then 1
            else date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month)
          end as n_12_month,

          case 
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) >= 6 then 6
            when date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month) = 0 then 1
            else date_diff(ifnull(if(a.deletion_date_dt < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', a.deletion_date_dt, null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(a.creation_date), interval 7 day), month)
          end as n_6_month,           
          
          date_add(a.creation_date_dt, interval 7 day) as creation_date
               
        from refined.analytical_rt_establishments_actual a
        join `refined.customer_base_establishment`  c 
          on c.establishment_sfid = a.salesforce_id
        where c.Reservation_Tool_createdDate is not null 
          and (a.deletion_date_dt >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or a.deletion_date_dt is null)
          and date(a.creation_date) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
)


, login  as (

        select 
          * 
        from `refined.as_absolute_base_adobe` 
        where product_type = 'reservation_tool'
          and month < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and month >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year) 
)


, login_actual as (

        select distinct 
          e.salesforce_id, 
          e.country_code, 
          el.month hit_month, 
          e.n_6_month,
          e.n_12_month,
          if(el.month is not null, sum(el.login), 0) as login_rt
            
        from  est e
        left join login el 
          on e.salesforce_id = el.establishment_sfid 
        group by 1,2,3,4,5
)

 
, rt_monthly_login_last_1m as (

      select distinct 
        salesforce_id, 
        country_code , 
        login_rt monthly_login_last_1m
            
      from login_actual
      where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month) 
        or hit_month is null
 
)

, rt_monthly_login_last_6m as (  

      select 
        salesforce_id, 
        country_code, 
        round(sum(login_rt)/n_6_month,2) as monthly_login_last_6m
            
      from login_actual
      where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month) 
        or hit_month is null
      group by 1, 2, n_6_month

)


, rt_monthly_login_last_1y as (  

      select 
        salesforce_id, 
        country_code, 
        round(sum(login_rt)/n_12_month, 2) as monthly_login_last_1y
            
      from login_actual
      where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 year) 
        or hit_month is null
      group by 1, 2, n_12_month

)
      

, establishment_login as ( 

      select 
        salesforce_id, 
        coalesce(a.country_code, b.country_code, c.country_code) as country_code,
        ifnull(c.monthly_login_last_1m, 0) as monthly_login_last_1m, 
        a.monthly_login_last_1y, 
        ifnull(b.monthly_login_last_6m, 0) as monthly_login_last_6m,
        round(0.5*ifnull(c.monthly_login_last_1m,0) + 
              0.2*ifnull(a.monthly_login_last_1y,0) + 
              0.3*ifnull(b.monthly_login_last_6m,0),2) as agg_login_rt,
        'a' as t
             
      from rt_monthly_login_last_1y a
      full outer join rt_monthly_login_last_6m b using (salesforce_id)
      full outer join rt_monthly_login_last_1m c using (salesforce_id)  
      
)     


, stat_login as (

      select distinct 
        country_code, 
        round(avg(agg_login_rt) over(partition by country_code),5) as mean_login_country,
        round(stddev(agg_login_rt) over(partition by country_code),5) as sd_login_country,
        round(percentile_cont(agg_login_rt, 0.5) over(partition by country_code),5) as median_login_country,

      from establishment_login
      where agg_login_rt != 0 
)

, int_stat_login as (

      select distinct 
        t, 
        round(avg(agg_login_rt) over(partition by t),5) as int_mean_login,
        round(stddev(agg_login_rt) over(partition by t),5) as int_sd_login,
        round(percentile_cont(agg_login_rt, 0.5) over(partition by t),5) as int_median_login,

      from establishment_login
      where agg_login_rt != 0 
)


      select distinct 
        salesforce_id, 
        a.country_code, 
        monthly_login_last_1m, 
        monthly_login_last_6m, 
        monthly_login_last_1y,  
        agg_login_rt, 
        ifnull(mean_login_country,0) as mean_login_country, 
        ifnull(sd_login_country, 0) as sd_login_country, 
        ifnull(median_login_country, 0) as median_login_country,
    
        case 
          when agg_login_rt = 0 then 0
          when agg_login_rt < mean_login_country then 1
          when agg_login_rt between mean_login_country and (mean_login_country + sd_login_country) then 2
          else 3
        end country_RT_login_score_rel,
      
      int_mean_login as int_mean_login_rt, 
      int_sd_login, 
      int_median_login,
      
      case 
        when agg_login_rt = 0 then 0
        when agg_login_rt < int_mean_login then 1
        when agg_login_rt between int_mean_login and (int_mean_login + int_sd_login) then 2
        else 3
      end as int_RT_login_score_rel,      
      
      date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month
        
from establishment_login a
left join stat_login using (country_code)
left join int_stat_login using (t)  
    """,
        project,
    )

def query_as_absolute_wl_login(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

est as (  

        select distinct 
          c.establishment_sfid as salesforce_id,
          date(c.Web_Listing_createdDate) as org_creation_date, 
          date(c.Web_Listing_deletedDate),
          countryCode as country_code, 
          # '2020-10-01' is the start date when WL is tracked in adobe
          case 
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2020-10-01', date_add(date(c.Web_Listing_createdDate), interval 7 day)), month)  >= 12 then 12
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2020-10-01', date_add(date(c.Web_Listing_createdDate), interval 7 day)), month) = 0 then 1
            else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2020-10-01', date_add(date(c.Web_Listing_createdDate), interval 7 day)), month)
          end as n_12_month,

          case 
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2020-10-01', date_add(date(c.Web_Listing_createdDate), interval 7 day)), month)  >= 12 then 12
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2020-10-01', date_add(date(c.Web_Listing_createdDate), interval 7 day)), month) = 0 then 1
            else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2020-10-01', date_add(date(c.Web_Listing_createdDate), interval 7 day)), month)
          end as n_6_month,   
          date_add(date(c.Web_Listing_createdDate), interval 7 day) creation_date
               
        from `refined.customer_base_establishment`  c 
        left join trusted_views.awl_business_unit a
          on c.establishment_sfid = a.establishmentId
        where c.Web_Listing_createdDate is not null 
          and (date(c.Web_Listing_deletedDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or c.Web_Listing_deletedDate is null)  # take all establishments that still exist until '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and date(c.Web_Listing_createdDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' # take establishments created before '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
)

, adobe  as (

        select 
          * 
        from `refined.as_absolute_base_adobe` 
        where product_type = 'web_listing'
          and month < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and month >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year) 
)

, login_actual as (

        select distinct 
          e.salesforce_id, 
          e.country_code, 
          el.month hit_month, 
          e.n_6_month, 
          e.n_12_month,
          if(el.month is not null, sum(el.login), 0) login
            
        from  est e
        left join adobe el 
          on e.salesforce_id = el.establishment_sfid 
        group by 1,2,3,4,5
)

 
, monthly_login_last_1m as (

        select 
          salesforce_id, 
          country_code, 
          sum(login) monthly_login_last_1m
              
        from login_actual
        where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month) 
          or hit_month is null
        group by 1,2
 
)

, monthly_login_last_6m as (  

        select 
          salesforce_id, 
          country_code, 
          round(sum(login)/n_6_month,2) as monthly_login_last_6m
              
        from login_actual
        where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month) 
          or hit_month is null
        group by 1, 2, n_6_month

)

, monthly_login_last_1y as (  

        select 
          salesforce_id, 
          country_code, 
          round(sum(login)/n_12_month, 2) monthly_login_last_1y
              
        from login_actual
        where hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 year) 
          or hit_month is null
        group by 1, country_code, n_12_month

)
      


, establishment_login as ( 

        select 
          a.salesforce_id, 
          coalesce(a.country_code, b.country_code, c.country_code) as country_code,
          ifnull(c.monthly_login_last_1m, 0) monthly_login_last_1m, 
          a.monthly_login_last_1y, ifnull(b.monthly_login_last_6m, 0 ) monthly_login_last_6m,
          round(0.5*ifnull(c.monthly_login_last_1m,0) + 
                0.2*ifnull(a.monthly_login_last_1y,0) + 
                0.3*ifnull(b.monthly_login_last_6m,0),2) as agg_login_rel, # agg_login for relative AS, will be set as agg_login if the threshold for WL login > 1 yearly
          a.monthly_login_last_1y as agg_login, # for absolute AS if the threshold for WL login = 1 yearly
          'a' as t
              
        from monthly_login_last_1y a
        full outer join monthly_login_last_6m b using (salesforce_id)
        full outer join monthly_login_last_1m c using (salesforce_id)  
      
)     


, stat_login as (

        select distinct 
          country_code, 
          round(avg(agg_login_rel) over (partition by country_code),5) as mean_login,
          round(stddev(agg_login_rel) over (partition by country_code),5) as sd_login,
          round(percentile_cont(agg_login_rel, 0.5) over (partition by country_code),5) as median_login,

        from establishment_login
        where agg_login != 0 
)

, int_stat_login as (

        select distinct 
          t, 
          round(avg(agg_login_rel) over (partition by t),5) as int_mean_login_WL,
          round(stddev(agg_login_rel) over (partition by t),5) as int_sd_login_WL,
          round(percentile_cont(agg_login_rel, 0.5) over (partition by t),5) as int_median_login_WL,

        from establishment_login
        where agg_login != 0 
)

        select distinct 
          a.salesforce_id, 
          a.country_code,
          a.monthly_login_last_1y, 
          a.monthly_login_last_6m , 
          a.monthly_login_last_1m,
          a.agg_login as agg_login_WL, 
          
          int.int_mean_login_WL, 
          int.int_sd_login_WL, 
          int.int_median_login_WL,
          
          ifnull(c.mean_login,0) mean_login_WL, 
          ifnull(c.sd_login,0) sd_login_WL,
          ifnull(c.median_login,0) median_login_WL,
              
          case 
            when a.agg_login_rel = 0 then 0
            when a.agg_login_rel < int.int_mean_login_WL  then 1
            when a.agg_login_rel between int.int_mean_login_WL and (int.int_mean_login_WL + int.int_sd_login_WL) then 2
            else 3
          end as int_WL_login_score_rel,
        
          case 
            when a.agg_login_rel = 0 then 0
            when a.agg_login_rel < c.mean_login  then 1
            when a.agg_login_rel between c.mean_login and (c.mean_login + c.sd_login) then 2
            else 3
          end as country_WL_login_score_rel,

          date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month
          
  from establishment_login a
  left join int_stat_login int using (t)
  left join stat_login c using (country_code)
    """,
        project,
    )

def query_as_absolute_mk_login(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

mk as (
          select distinct
            a.branch_office_id, 
            a.country as country_code, 
            a.establishment_sfid,
            date(a.Menu_Kit_createdDate) org_creation_date, 
            date_add(date(a.Menu_Kit_createdDate), interval 7 day) creation_date, 
            case 
              when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date_add(date(a.Menu_Kit_createdDate), interval 7 day), month) >= 12 then 12
              when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date_add(date(a.Menu_Kit_createdDate), interval 7 day), month) = 0 then 1
              else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date_add(date(a.Menu_Kit_createdDate), interval 7 day), month)
            end n_12_month,

            case 
              when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date_add(date(a.Menu_Kit_createdDate), interval 7 day), month) >= 6 then 6
              when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date_add(date(a.Menu_Kit_createdDate), interval 7 day), month) = 0 then 1
              else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date_add(date(a.Menu_Kit_createdDate), interval 7 day), month)
            end n_6_month
          from `refined.customer_base_establishment`  a
          where a.Menu_Kit_createdDate is not null
            and date(a.Menu_Kit_createdDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
)


, adobe as (

          select distinct 
            mk.establishment_sfid salesforce_id, 
            branch_office_id, 
            mk.creation_date, 
            u.date, 
            u.login_mk logins, 
            u.hit_month,
            max(u.last_date_mk_login) over (partition by mk.branch_office_id, mk.establishment_sfid) date_last_activity_mk_login
          from  mk mk
          join refined.as_absolute_base_adobe_mk u 
            on branchoffice_id = cast(mk.branch_office_id as string)  
              or branchoffice_id = establishment_sfid
              or u.salesforce_id = mk.establishment_sfid    
          group by 1,2,3,4,5,6, last_date_mk_login
)


, login_est as (

          select 
            *
          from adobe
          where date > creation_date -- take only logins after 7 days of creation date
            and date >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 year) 
            and date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' # take 1 year logins until end of set date

)

, login_actual as (

          select 
            salesforce_id, 
            branch_office_id, 
            country_code, 
            date_last_activity_mk_login,
            round(sum(if(hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -12 month), logins, 0))/n_12_month, 2) as monthly_login_last_1y,
            round(sum(if(hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month), logins, 0))/n_6_month, 2) as monthly_login_last_6m,
            sum(if(hit_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month), logins, 0)) as monthly_login_last_1m,

          from (
                    select distinct
                      e.establishment_sfid salesforce_id, 
                      e.branch_office_id, 
                      e.country_code, 
                      el.logins, 
                      n_12_month, 
                      n_6_month, 
                      el.hit_month, 
                      el.date,
                      a.date_last_activity_mk_login

                    from  mk e
                    left join login_est el 
                      on ifnull(e.establishment_sfid, 'non') = ifnull(el.salesforce_id, 'non') 
                        and ifnull(cast(e.branch_office_id as string), 'non') = ifnull(cast(el.branch_office_id as string), 'non')
                    left join adobe a
                      on ifnull(e.establishment_sfid, 'non') = ifnull(a.salesforce_id, 'non') 
                        and ifnull(cast(e.branch_office_id as string), 'non') = ifnull(cast(a.branch_office_id as string), 'non')      
                )
          
          group by salesforce_id, branch_office_id, country_code, n_12_month, n_6_month, date_last_activity_mk_login

)

, agg_logins as (

          select distinct 
            salesforce_id, 
            branch_office_id, 
            country_code, 
            'a' t, 
            date_last_activity_mk_login,
            ifnull(monthly_login_last_1y, 0) monthly_login_last_1y, 
            ifnull(monthly_login_last_6m, 0) monthly_login_last_6m, 
            ifnull(monthly_login_last_1m, 0) monthly_login_last_1m,
            ifnull(round(0.5*ifnull(monthly_login_last_1m, 0) + 
                         0.3*ifnull(monthly_login_last_6m, 0) + 
                         0.2*monthly_login_last_1y,2), 0) agg_login_rel, # agg_login for relative AS, will be used if the threshold for Website login > 1 yearly,
            monthly_login_last_1y as agg_login # for absolute AS if the threshold for Website login = 1 yearly

          from login_actual 
)

, stat_login as (


          select distinct 
            country_code , 
            round(avg(agg_login_rel) over (partition by country_code),5) as mean_login,
            round(stddev(agg_login_rel) over (partition by country_code),5) as sd_login, 
            round(percentile_cont(agg_login_rel, 0.5) over (partition by country_code),5) as median_login,
                       
            round(avg(agg_login_rel) over (partition by t),5) as int_mean_login,
            round(stddev(agg_login_rel) over (partition by t),5) as int_sd_login,
            round(percentile_cont(agg_login_rel, 0.5) over (partition by t),5) as int_median_login,    

          from agg_logins
          where agg_login_rel != 0 
)


          select distinct 
            a.salesforce_id, 
            a.branch_office_id, 
            a.country_code, 
            a.monthly_login_last_1y, 
            a.monthly_login_last_6m, 
            a.monthly_login_last_1m,
            a.agg_login as agg_login_MK,
            ifnull(b.int_mean_login, 0) as int_mean_login_MK,
            ifnull(b.int_sd_login, 0) as int_sd_login_MK,
            ifnull(b.int_median_login, 0) as int_median_login_MK,
            ifnull(b.mean_login, 0) as mean_login_MK,
            ifnull(b.sd_login, 0) as sd_login_MK,     
            ifnull(b.int_median_login, 0) as median_login_MK,

            case 
              when a.agg_login_rel = 0 then 0
              when a.agg_login_rel < b.int_mean_login then 1
              when a.agg_login_rel between b.int_mean_login and (b.int_mean_login + b.int_sd_login) then 2
              else 3
            end as int_MK_login_score_rel,

            case 
              when a.agg_login_rel = 0 then 0
              when a.agg_login_rel < b.mean_login  then 1
              when a.agg_login_rel between b.mean_login and (b.mean_login + b.sd_login) then 2
              else 3
            end as country_MK_login_score_rel,
            
            date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as_month,
            date_last_activity_mk_login


        from agg_logins a
        left join stat_login b
        using (country_code)   
    """,
        project,
    )

def query_as_absolute_sfdc_call(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with 

base as (

      select distinct 
        e.account_id, 
        a.sfdc_internal_account_id, 
        cb.establishment_sfid as establishment_id, 
        cb.SFDC_createdDate, 
        date(cb.SFDC_deletedDate) as SFDC_deletedDate, 
        e.country_code,
        case 
          when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) >= 12 then 12
          when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) = 0 then 1
          else date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month)
        end as n_12_month,

        case 
          when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) >= 6 then 6
          when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) = 0 then 1
          else date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month)
        end as n_6_month,      

      from `refined.analytical_sfdc_establishment_actual` e
      join refined.analytical_sfdc_account_actual a
        on e.account_id = a.account_id 
      right join `refined.customer_base_establishment` cb
        on e.establishment_id = cb.establishment_sfid 
      where SFDC_createdDate is not null
        and date(SFDC_createdDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
        and (date(SFDC_deletedDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or SFDC_deletedDate is null)  # take all establishments that still exist until '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
        and has_Platform_CMS + has_Reservation_Tool + has_Web_Listing + has_Order_Tool + has_Menu_Kit + has_Lynn + has_MenuEngineering + has_MTO > 0

      
) 

, call_data as (

      select  
        establishment_id, 
        CallType,
        date_trunc(date(a.NVM_Time__c), month) as date_month, 
        NVMContactWorld__CallTalkTimeInSeconds__c as talktime,
        n_12_month,
        n_6_month
      from `trusted_views.asfdc_task` a
      inner join base 
        on a.AccountId = base.sfdc_internal_account_id 
      where (
              (CallType='Inbound' and a.NVMContactWorld__CallTalkTimeInSeconds__c > 10) 
            or (CallType ='Outbound' and not (a.Call_Outcome__c is null or a.Call_Outcome__c ='Wrong Number'))
            )
        and date(a.NVM_Time__c ) between date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 year) and date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 day) -- deliveries of 1 year until set date

)

, last_call as (

      select  
        establishment_id,
        max(date(a.NVM_Time__c)) as date_last_inbound_call

      from `trusted_views.asfdc_task` a
      inner join base 
        on a.AccountId = base.sfdc_internal_account_id 
      where (CallType='Inbound' and a.NVMContactWorld__CallTalkTimeInSeconds__c > 10) 
        and date(a.NVM_Time__c ) between date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 year) and current_date 
      group by 1            

)


, calls_last_1y as (

      select 
        establishment_id, 
        max(nIn_calls_last_1y)/n_12_month as nIn_calls_last_1y, 
        max(nOut_calls_last_1y) as nOut_calls_last_1y, 
        max(nOut_all_last_1y) as nOut_all_last_1y, 
      from (
            select 
              establishment_id, 
              n_12_month,
              if(CallType = 'Inbound', count(date_month), 0) as nIn_calls_last_1y,  
              if(CallType = 'Outbound', countif(call_data.talktime  > 10),0) as nOut_calls_last_1y,
              if(CallType = 'Outbound', count(date_month), null) as nOut_all_last_1y,  
      
            from call_data 
            where date_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -12 month)
            group by 1, CallType, n_12_month
            )
     group by 1, n_12_month
 
     
 )

, calls_last_6m as (

      select 
        establishment_id, 
        max(nIn_calls_last_6m)/n_6_month as nIn_calls_last_6m, 
        max(nOut_calls_last_6m) as nOut_calls_last_6m, 
        max(nOut_all_last_6m) as nOut_all_last_6m

      from (
            select distinct 
              establishment_id, 
              n_6_month,
              if(CallType = 'Inbound', count(date_month), 0) as nIn_calls_last_6m,  
              if(CallType = 'Outbound', countif(call_data.talktime  > 10),0) as nOut_calls_last_6m,
              if(CallType = 'Outbound', count(date_month), null) as nOut_all_last_6m,  
            
            from call_data 
            where date_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month)
            group by 1, CallType, n_6_month
           )
      group by 1, n_6_month
      
)

, calls_last_1m as (

      select 
        establishment_id, 
        max(nIn_calls_last_1m) nIn_calls_last_1m, 
        max(nOut_calls_last_1m) nOut_calls_last_1m, 
        max(nOut_all_last_1m) nOut_all_last_1m

      from (
            select 
              establishment_id, 
              if(CallType = 'Inbound', count(date_month), 0) as nIn_calls_last_1m,  
              if(CallType = 'Outbound', countif(call_data.talktime  > 10),0) as nOut_calls_last_1m,
              if(CallType = 'Outbound', count(date_month), null) as nOut_all_last_1m, 
            from call_data 
            where date_month >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month)
            group by 1, CallType
            )
      group by 1
      
)

, agg_calls_prep as (

      select 
        base.establishment_id, 
        country_code,
        nIn_calls_last_1y, 
        nIn_calls_last_6m, 
        nIn_calls_last_1m,
        nOut_all_last_1y, 
        nOut_calls_last_1y, 
        nOut_all_last_6m, 
        nOut_calls_last_6m, 
        nOut_all_last_1m , 
        nOut_calls_last_1m,
        if(nOut_calls_last_6m is null, 0, 1) has_call_6m,
        if(nOut_calls_last_1m is null, 0, 1) has_call_1m,
        case when nOut_all_last_1y is null then null else nOut_calls_last_1y / nOut_all_last_1y end nOut_rel_last_1y,
        case when nOut_all_last_6m is null then null else nOut_calls_last_6m / nOut_all_last_6m end nOut_rel_last_6m,
        case when nOut_all_last_1m is null then null else nOut_calls_last_1m / nOut_all_last_1m end nOut_rel_last_1m,

      from calls_last_1y  a
      full join calls_last_6m  using (establishment_id)
      full join calls_last_1m  using (establishment_id) 
      right join base using (establishment_id) 

      
)

, agg_calls as (

      select 
        agg_calls_prep.*,
        0.5*ifnull(nIn_calls_last_1m,0) + 
        0.3*ifnull(nIn_calls_last_6m,0) + 
        0.2*ifnull(nIn_calls_last_1y,0) agg_Inbound,

        case 
          when nOut_all_last_1y is null then null
          else
              (0.5*ifnull(nOut_rel_last_1m,0)*has_call_1m + 
               0.3*ifnull(nOut_rel_last_6m, 0)*has_call_6m + 
               0.2*ifnull(nOut_rel_last_1y,0))
              /
              (0.5*has_call_1m + 
               0.3*has_call_6m + 
               0.2)
        end agg_Outbound,           
        'a' t
                       
      from agg_calls_prep
      
)


, stat_In_calls as (

      select 
        country_code, 
        mean_Inbound, 
        sd_Inbound, 
        median_Inbound

      from (
            select distinct 
              country_code,
              round(avg(agg_Inbound) over (partition by country_code),5) as mean_Inbound,
              round(stddev(agg_Inbound) over (partition by country_code),5) as sd_Inbound,
              round(percentile_cont(agg_Inbound, 0.5) over (partition by country_code),5) as median_Inbound
            from agg_calls 
            where agg_Inbound != 0 
           )
           
)
  
, stat_Out_calls as (

      select 
        country_code, 
        mean_Outbound, 
        sd_Outbound, 
        median_Outbound

      from (
            select distinct 
              country_code,
              round(avg(agg_Outbound) over (partition by country_code),5) as mean_Outbound,
              round(stddev(agg_Outbound) over (partition by country_code),5) as sd_Outbound,
              round(percentile_cont(agg_Outbound, 0.5) over (partition by country_code),5) as median_Outbound

            from agg_calls 
            where agg_Outbound != 0 and agg_Outbound is not null
            )
            
)  

, int_stat_In_calls as (

      select 
        t, 
        int_mean_Inbound, 
        int_sd_Inbound, 
        int_median_Inbound

      from (
            select distinct 
              t,
              round(avg(agg_Inbound) over (partition by t),5) as int_mean_Inbound,
              round(stddev(agg_Inbound) over (partition by t),5) as int_sd_Inbound,
              round(percentile_cont(agg_Inbound, 0.5) over (partition by t),5) as int_median_Inbound
            from agg_calls 
            where agg_Inbound != 0 
           )
           
)
  
, int_stat_Out_calls as (

      select 
        t, 
        int_mean_Outbound, 
        int_sd_Outbound, 
        int_median_Outbound
      from (
            select distinct 
              t,
              round(avg(agg_Outbound) over (partition by t),5) as int_mean_Outbound,
              round(stddev(agg_Outbound) over (partition by t),5) as int_sd_Outbound,
              round(percentile_cont(agg_Outbound, 0.5) over (partition by t),5) as int_median_Outbound
            from agg_calls 
            where agg_Outbound != 0 
              and agg_Outbound is not null 
            )
            
)

      select distinct
        establishment_id as salesforce_id, 
        a.country_code,
        cast(nIn_calls_last_1y as FLOAT64) as nIn_calls_last_1y, 
        cast(nIn_calls_last_6m as FLOAT64) as nIn_calls_last_6m, 
        cast(nIn_calls_last_1m as FLOAT64) as nIn_calls_last_1m,
        nOut_rel_last_1y,
        nOut_rel_last_6m,
        nOut_rel_last_1m,
        agg_Outbound,
        mean_Outbound, 
        sd_Outbound,
        median_Outbound,
        int_mean_Inbound, int_sd_Inbound, int_median_Inbound,             
        int_mean_Outbound, int_sd_Outbound, int_median_Outbound,           
        agg_Inbound, ifnull(mean_Inbound, 0) mean_Inbound, ifnull(sd_Inbound, 0) sd_Inbound, ifnull(median_Inbound, 0) median_Inbound,
        case when agg_Inbound = 0 then 0
            when agg_Inbound < mean_Inbound then 1
            when agg_Inbound between mean_Inbound and (mean_Inbound + sd_Inbound) then 2
            else 3
        end as country_SFDC_Inbound_score_rel, 
        case when agg_Inbound = 0 then 0
            when agg_Inbound < int_mean_Inbound then 1
            when agg_Inbound between int_mean_Inbound and (int_mean_Inbound + int_sd_Inbound) then 2
            else 3
        end as int_SFDC_Inbound_score_rel, 
        case when agg_Outbound is null then null
            when agg_Outbound = 0 then 0
            when agg_Outbound < mean_Outbound then 1
            when agg_Outbound between mean_Outbound and (mean_Outbound + sd_Outbound) then 2
            else 3
        end as country_SFDC_Outbound_score_rel,     
        case when agg_Outbound is null then null
            when agg_Outbound = 0 then 0
            when agg_Outbound < int_mean_Outbound then 1
            when agg_Outbound between int_mean_Outbound and (int_mean_Outbound + int_sd_Outbound) then 2
            else 3
        end as int_SFDC_Outbound_score_rel,
        date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as_month,
        date_last_inbound_call
      from agg_calls a 
      left join stat_In_calls using (country_code)
      left join stat_Out_calls using (country_code)
      left join int_stat_In_calls using (t)
      left join int_stat_Out_calls using (t)
      left join last_call using (establishment_id)
    """,
        project,
    )

def query_as_absolute_do_login_event(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with 

est AS (

        select 
          sfdc_establishment_id, 
          cntr.iso_code_2 as country_code,
          min(created_at) Order_Tool_createdDate, # an establishment max( different creation date
          date_add(date(min(created_at)), interval 7 day) creation_date,

          case 
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2021-06-01',date_add(date(min(created_at)), interval 7 day)), month) >= 3 then 3
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2021-06-01',date_add(date(min(created_at)), interval 7 day)), month) = 0 then 1
            else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2021-06-01',date_add(date(min(created_at)), interval 7 day)), month)
          end n_3_month,

          case 
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2021-06-01',date_add(date(min(created_at)), interval 7 day)), month) >= 6 then 6
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2021-06-01',date_add(date(min(created_at)), interval 7 day)), month) = 0 then 1
            else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', greatest('2021-06-01',date_add(date(min(created_at)), interval 7 day)), month)
          end n_6_month,
               
        from `refined.analytical_order_establishments_actual` est      
        left join `trusted_views.aorder_countries` cntr
          on est.country = cntr.country_name
        
        where date(created_at) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and (date(est.SFDC_DisabledDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
                or est.SFDC_DisabledDate is null)
          and date(created_at) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and sfdc_establishment_id is not null                     
        group by 1,2
)


, adobe  as (

        select 
          * 
        from `refined.as_absolute_base_adobe` 
        where lower(product_type) in ('order_tool', 'order_tool_admin')
          and month < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and month >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 6 month) 
        
)


, events as (

        select
          establishment_salesforce_id, 
          event_month, 
          creation_date,  
          count(event) event

        from (
                select distinct
                  sfdc_establishment_id as establishment_salesforce_id,
                  derived_event_date as event_date,  
                  date_trunc(derived_event_date, month) as event_month,
                  event, 
                  creation_date

                from refined.analytical_order_derived_events x
                join est
                  using (sfdc_establishment_id)
                where derived_event_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
                  and derived_event_date >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 6 month) 
                  and derived_event_date > creation_date
              )
        group by 1,2,3
     
)


, last_date_event as (

        select  
          sfdc_establishment_id as salesforce_id,
          max(derived_event_date) as date_last_activity_do_event

        from refined.analytical_order_derived_events
        group by 1
)                


, login_event as (

        select distinct
          ifnull(u.establishment_sfid, ev.establishment_salesforce_id) salesforce_id, 
          ifnull(u.login, 0) logins, 
          ifnull(ev.event, 0) events, 
          ifnull(u.month, ev.event_month) hit_month

        from adobe u 
        full join events ev 
          on u.establishment_sfid = ev.establishment_salesforce_id 
            and month = event_month
)


, login_event_actual as (

        select 
          e.sfdc_establishment_id as salesforce_id, 
          e.country_code ,   
          n_3_month, 
          n_6_month,
          round(
            sum(
              if(le.hit_month >= date_add(date_trunc(current_date(), month), interval -3 month), le.logins, 0)
                )
            /
            e.n_3_month, 2) as monthly_login_last_3m,

          round(
            sum(
              if(le.hit_month >= date_add(date_trunc(current_date(), month), interval -6 month), le.logins, 0)
                )
            /
            e.n_6_month, 2) as monthly_login_last_6m,

          sum(
            if(le.hit_month >= date_add(date_trunc(current_date(), month), interval -1 month), le.logins, 0)
              ) as monthly_login_last_1m,
          
          round(
            sum(
              if(le.hit_month >= date_add(date_trunc(current_date(), month), interval -3 month), le.events, 0)
                ) 
            / 
            e.n_3_month, 2) as monthly_event_last_3m,

          round(
            sum(
              if(le.hit_month >= date_add(date_trunc(current_date(), month), interval -6 month), le.events, 0)
                )
            /
            e.n_6_month, 2) as monthly_event_last_6m,

          sum(
            if(le.hit_month >= date_add(date_trunc(current_date(), month), interval -1 month), events, 0)
            ) as monthly_event_last_1m,
                
        from  est e
        left join login_event le 
          on e.sfdc_establishment_id = le.salesforce_id
        group by e.sfdc_establishment_id, e.country_code, n_3_month, n_6_month
)


, agg_login_event as (

        select distinct 
          salesforce_id, 
          country_code, 
          'a' t,
          ifnull(monthly_login_last_3m, 0) as monthly_login_last_3m, 
          ifnull(monthly_login_last_6m, 0) as monthly_login_last_6m, 
          ifnull(monthly_login_last_1m, 0) as monthly_login_last_1m,
          ifnull(round(0.5*ifnull(monthly_login_last_1m, 0) + 
                       0.3*ifnull(monthly_login_last_3m, 0) + 
                       0.2*monthly_login_last_6m,2), 0) as agg_login,
          
          ifnull(monthly_event_last_3m, 0) as monthly_event_last_3m, 
          ifnull(monthly_event_last_6m, 0) as monthly_event_last_6m, 
          ifnull(monthly_event_last_1m, 0) as monthly_event_last_1m,
          ifnull(round(0.5*ifnull(monthly_event_last_1m, 0) + 
                       0.3*ifnull(monthly_event_last_3m, 0) + 
                       0.2*monthly_event_last_6m,2), 0) as agg_event,              

        from login_event_actual  

)

, stat_login as (

        select distinct 
          country_code , 
          round(avg(agg_login) over (partition by country_code),5) as mean_login,
          round(stddev(agg_login) over (partition by country_code),5) as sd_login, 
          round(percentile_cont(agg_login, 0.5) over (partition by country_code),5) as median_login,
          
          round(avg(agg_login) over (partition by t),5) as int_mean_login,
          round(stddev(agg_login) over (partition by t),5) as int_sd_login,
          round(percentile_cont(agg_login, 0.5) over (partition by t),5) as int_median_login,

        from agg_login_event
        where agg_login != 0 
)

, stat_event as (

        select distinct 
          country_code , 
          round(avg(agg_event) over (partition by country_code),5) as mean_event,
          round(stddev(agg_event) over (partition by country_code),5) as sd_event, 
          round(percentile_cont(agg_event, 0.5) over (partition by country_code),5) as median_event,
          
          round(avg(agg_event) over (partition by t),5) as int_mean_event,
          round(stddev(agg_event) over (partition by t),5) as int_sd_event,
          round(percentile_cont(agg_event, 0.5) over (partition by t),5) as int_median_event,

        from agg_login_event
        where agg_event != 0 
)


        select 
          a.salesforce_id, 
          a.country_code,  
          a.monthly_login_last_6m, 
          a.monthly_login_last_3m,               
          a.monthly_login_last_1m,
          a.agg_login agg_login_DO,
          ifnull(b.int_mean_login, 0) int_mean_login_DO ,
          ifnull(b.int_sd_login, 0) int_sd_login_DO,
          ifnull(b.int_median_login, 0) int_median_login_DO,
          
          ifnull(b.mean_login, 0) mean_login_DO,
          ifnull(b.sd_login, 0) sd_login_DO,    
          ifnull(b.median_login, 0) median_login_DO,
          
          a.monthly_event_last_6m, 
          a.monthly_event_last_3m,                
          a.monthly_event_last_1m,
          a.agg_event agg_event_DO,
          ifnull(c.int_mean_event, 0) int_mean_event_DO,
          ifnull(c.int_sd_event, 0) int_sd_event_DO,
          ifnull(c.int_median_event, 0) int_median_event_DO,
          
          ifnull(c.mean_event, 0) mean_event_DO,
          ifnull(c.sd_event, 0) sd_event_DO,        
          ifnull(c.median_event, 0) median_event_DO,

          case 
            when a.agg_login = 0 then 0
            when a.agg_login < b.int_mean_login  then 1
            when a.agg_login between b.int_mean_login and (b.int_mean_login + b.int_sd_login) then 2
            else 3
          end int_DO_login_score_rel,

          case 
            when a.agg_login = 0 then 0
            when a.agg_login < b.mean_login  then 1
            when a.agg_login between b.mean_login and (b.mean_login + b.sd_login) then 2
            else 3
          end country_DO_login_score_rel,
          

          case 
            when a.agg_event = 0 then 0
            when a.agg_event < c.int_mean_event  then 1
            when a.agg_event between c.int_mean_event and (c.int_mean_event + c.int_sd_event) then 2
            else 3
          end int_DO_event_score_rel,

          case 
            when a.agg_event = 0 then 0
            when a.agg_event < c.mean_event  then 1
            when a.agg_event between c.mean_event and (c.mean_event + c.sd_event) then 2
            else 3
          end country_DO_event_score_rel,
          
          date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month,
          d.date_last_activity_do_event                
                  

        from agg_login_event a
        left join stat_login b
          using (country_code)
        left join stat_event c
          using (country_code)
        left join last_date_event d
          using (salesforce_id)   
    """,
        project,
    )

def query_as_absolute_do_order(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

est as (

        select 
          sfdc_establishment_id as establishment_sfid, 
          cntr.iso_code_2 as country_code, 
          min(created_at) creation_date, # an establishment max( different creation date
          
          case 
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(min(created_at)), month) >= 3 then 3
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(min(created_at)), month) = 0 then 1
            else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(min(created_at)), month)
          end n_3_month,

          case 
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(min(created_at)), month) >= 6 then 6
            when date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(min(created_at)), month) = 0 then 1
            else date_diff('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(min(created_at)), month)
          end n_6_month,
               
        from `refined.analytical_order_establishments_actual` est      
        left join `trusted_views.aorder_countries` cntr
          on est.country = cntr.country_name       
        where date(created_at) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and (date(est.SFDC_DisabledDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or est.SFDC_DisabledDate is null)  # take all establishments that still exist until '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
          and date(created_at) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' # take establishments created before '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and sfdc_establishment_id is not null                       
        group by 1,2
        
)

, orders as (

        select 
          establishment_sfid, 
          date(order_date) order_date, 
          if(order_status in ("accepted", 
                              "cancelled", 
                              "completed"), count(*), 0) n_reacted_order,
          count(*) total_order

        from (
                select distinct 
                  sfdc_establishment_id as establishment_sfid, 
                  order_id, 
                  order_status, 
                  date_added as order_date, 
                  date_accepted

                from `refined.analytical_order_orders_actual`
                where date(date_added) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' 
                  and date(date_added) >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 6 month) 
                  and order_status is not null
                  and (not (regexp_contains(lower(comment), r"(\\s?test(\\s|\\d)*\\b)|testbestellung|testowe|testnachricht|testkauf")
                        and not regexp_contains(lower(comment), r"testé")) 
                        or comment is null)
                
         )
               
        group by 1,2, order_status

)

, last_date_order as (

                select 
                  sfdc_establishment_id as establishment_sfid, 
                  max(date(date_added)) as date_last_activity_order

                from `refined.analytical_order_orders_actual`
                where order_status is not null
                  and (not (regexp_contains(lower(comment), r"(\\s?test(\\s|\\d)*\\b)|testbestellung|testowe|testnachricht|testkauf")
                        and not regexp_contains(lower(comment), r"testé")) 
                        or comment is null)
                group by 1
                
         )
 

, order_est as (
  
        select 
          e.establishment_sfid, 
          e.country_code , 
          e.n_3_month, 
          e.n_6_month,               
          sum(if(o.order_date >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month), o.n_reacted_order, 0)) reacted_order_last_6m,
          sum(if(o.order_date >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -6 month), o.total_order, 0)) total_order_last_6m,
          
          sum(if(o.order_date >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -3 month), o.n_reacted_order, 0)) reacted_order_last_3m,
          sum(if(o.order_date >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -3 month), o.total_order, 0)) total_order_last_3m,
          
          sum(if(o.order_date >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month), o.n_reacted_order, 0)) reacted_order_last_1m,
          sum(if(o.order_date >= date_add('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval -1 month), o.total_order, 0)) total_order_last_1m,

        from est e
        left join orders o 
        using (establishment_sfid)
        group by 1,2,3,4  
)

, order_actual as (      
  
        select 
          establishment_sfid, 
          country_code, 
          n_3_month, 
          n_6_month,
                       
          ifnull(reacted_order_last_6m, 0) reacted_order_last_6m,
          ifnull(total_order_last_6m, 0) total_order_last_6m,    
          
          ifnull(reacted_order_last_3m, 0) reacted_order_last_3m,
          ifnull(total_order_last_3m, 0) total_order_last_3m,                       
          
          ifnull(reacted_order_last_1m, 0) reacted_order_last_1m,
          ifnull(total_order_last_1m, 0) total_order_last_1m,
          
          if(total_order_last_6m is null or total_order_last_6m = 0, 0, ifnull(reacted_order_last_6m, 0) / total_order_last_6m) rel_reacted_order_last_6m,
          if(total_order_last_3m is null or total_order_last_3m = 0, 0, ifnull(reacted_order_last_3m, 0) / total_order_last_3m) rel_reacted_order_last_3m,
          if(total_order_last_1m is null or total_order_last_1m = 0, 0, ifnull(reacted_order_last_1m, 0) / total_order_last_1m) rel_reacted_order_last_1m,
          
          round(ifnull(total_order_last_6m, 0)/n_6_month, 2) monthly_order_last_6m, 
          round(ifnull(total_order_last_3m, 0)/n_3_month, 2) monthly_order_last_3m,
          ifnull(total_order_last_1m, 0) monthly_order_last_1m,
          
          if(total_order_last_3m is null, 0, 1) has_order_3m,
          if(total_order_last_1m is null, 0, 1) has_order_1m
               
        from order_est
        
)


, agg_order as (

        select distinct 

          establishment_sfid, 
          "a" as t,
          country_code,
          (0.5*ifnull(rel_reacted_order_last_1m,0)*has_order_1m + 
           0.3*ifnull(rel_reacted_order_last_3m,0)*has_order_3m + 
           0.2*rel_reacted_order_last_6m) 
          /
          (0.5*has_order_1m + 
           0.3*has_order_3m + 
           0.2) as agg_rel_reacted,
          
          (0.5*ifnull(monthly_order_last_1m,0)*has_order_1m + 
           0.3*ifnull(monthly_order_last_3m,0)*has_order_3m + 
           0.2*monthly_order_last_6m)
          /
          (0.5*has_order_1m + 
           0.3*has_order_3m + 
           0.2) as agg_monthly_order,
          
          rel_reacted_order_last_6m,
          rel_reacted_order_last_3m,
          rel_reacted_order_last_1m,
          
          monthly_order_last_6m,
          monthly_order_last_3m,
          monthly_order_last_1m
              
        from order_actual

      
) 

, stat_orders_rel as (

        select distinct 

          country_code, 
          round(avg(agg_rel_reacted) over (partition by country_code),5) as mean_rel_reacted,
          round(stddev(agg_rel_reacted) over (partition by country_code),5) as sd_rel_reacted,
          round(percentile_cont(agg_rel_reacted, 0.5) over (partition by country_code),5) as median_rel_reacted,
          
          round(avg(agg_rel_reacted) over (partition by t),5) as int_mean_rel_reacted,
          round(stddev(agg_rel_reacted) over (partition by t),5) as int_sd_rel_reacted,
          round(percentile_cont(agg_rel_reacted, 0.5) over (partition by t),5) as int_median_rel_reacted,

        from agg_order
        where agg_rel_reacted != 0 
        
)

, stat_orders_abs as (

        select distinct 
          country_code, 
          round(avg(agg_monthly_order) over (partition by country_code),5) as mean_order,
          round(stddev(agg_monthly_order) over (partition by country_code),5) as sd_order,
          round(avg(agg_monthly_order) over (partition by t),5) as int_mean_order,
          round(stddev(agg_monthly_order) over (partition by t),5) as int_sd_order 

        from agg_order
        where agg_monthly_order != 0 
)

        select distinct 
          a.establishment_sfid as salesforce_id, 
          a.country_code, 

          a.rel_reacted_order_last_3m,
          a.rel_reacted_order_last_6m,
          a.rel_reacted_order_last_1m,
              
          a.monthly_order_last_3m,            
          a.monthly_order_last_6m,
          a.monthly_order_last_1m,
          
          a.agg_rel_reacted as agg_order_reacted_rel ,
          a.agg_monthly_order as agg_order,
          
          ifnull(rel.mean_rel_reacted, 0) as country_mean_rel_reacted, 
          ifnull(rel.sd_rel_reacted, 0) as country_sd_rel_reacted,
          ifnull(rel.median_rel_reacted, 0) as country_median_rel_reacted,
          rel.int_mean_rel_reacted, 
          rel.int_sd_rel_reacted,
          rel.int_median_rel_reacted,
          
          ifnull(ab.mean_order, 0) as country_mean_order, 
          ifnull(ab.sd_order, 0) as country_sd_order,
          ab.int_mean_order, 
          ab.int_sd_order,

          case 
            when a.agg_rel_reacted = 0 then 0
            when (a.agg_rel_reacted = 1 or a.agg_rel_reacted > rel.mean_rel_reacted) then 3
            when a.agg_rel_reacted between (rel.mean_rel_reacted - rel.sd_rel_reacted) and rel.mean_rel_reacted  then 2
            else 1
          end as country_DO_relReaction_score_rel,
          
          case 
            when a.agg_rel_reacted = 0 then 0
            when a.agg_rel_reacted > rel.int_mean_rel_reacted then 3
            when a.agg_rel_reacted between (rel.int_mean_rel_reacted - rel.int_sd_rel_reacted) and rel.int_mean_rel_reacted  then 2
            else 1
          end as int_DO_relReaction_score_rel,
          
          case 
            when a.agg_monthly_order = 0 then 0
            when a.agg_monthly_order < ab.mean_order then 1
            when a.agg_monthly_order between ab.mean_order and (ab.mean_order + ab.sd_order) then 2
            else 3
          end as country_DO_order_score_rel,

          case 
            when a.agg_monthly_order = 0 then 0
            when a.agg_monthly_order < ab.int_mean_order then 1
            when a.agg_monthly_order between ab.int_mean_order and (ab.int_mean_order + ab.int_sd_order) then 2
            else 3
          end as int_DO_order_score_rel,
          
          date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as_month,
          l.date_last_activity_order        

        from agg_order a
        left join stat_orders_rel rel using (country_code)
        left join stat_orders_abs ab using (country_code)
        left join last_date_order l using(establishment_sfid)
    """,
        project,
    )

def query_as_absolute_portal(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

base as (

        select distinct 
          cb.establishment_sfid, 
          cb.SFDC_createdDate, 
          date(cb.SFDC_deletedDate) as SFDC_deletedDate, 
          date_add(date(cb.SFDC_createdDate), interval 7 day) creation_date,
          cb.country,
          case 
            when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) >= 12 then 12
            when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) = 0 then 1
            else date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month)
          end as n_12_month,

          case 
            when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) >= 6 then 6
            when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) = 0 then 1
            else date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month)
          end as n_6_month

        from  `{project}.refined.customer_base_establishment` cb
        where cb.SFDC_createdDate is not null
          and date(cb.SFDC_createdDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and (date(cb.SFDC_deletedDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or cb.SFDC_deletedDate is null)  # take all establishments that still exist until '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'

)

, adobe_portal as (

        select distinct 
          visit_start_time_gmt_dt as visit_date,
          var_establishment_id, 
          var_business_id, 
          max(visit_start_time_gmt_dt) over (partition by var_establishment_id, var_business_id) as last_portal_login_date
        from `{project}.refined.adobe_datafeed` 

        where 
           ifnull(var_visitor_type,prop_visitor_type) = "restaurant owner" 
          and lower(ifnull(var_product_type,prop_product_type)) = "portal"
          and valid_hit = 1
)

, last_date as (

        select distinct 
          var_establishment_id, 
          var_business_id,
          last_portal_login_date

        from adobe_portal
)

, last_date_actual as(

        select distinct 
          cb.establishment_sfid,
          max(last_portal_login_date) as last_portal_login_date

        from last_date a
        join `{project}.refined.analytical_sfdc_establishment_actual` b
          on a.var_establishment_id = b.establishment_id 
            or a.var_business_id = b.establishment_id 
            or a.var_establishment_id = b.account_id
            or a.var_business_id = b.account_id
        join base cb
          on b.establishment_id = cb.establishment_sfid
        group by 1
)



, login_est as (

          select distinct 
            cb.establishment_sfid, 
            cb.country,
            cb.creation_date,
            #a.visit_date,
            n_12_month,
            count(distinct visit_date)/n_12_month as agg_portal_login,
            

          from adobe_portal a
          join `{project}.refined.analytical_sfdc_establishment_actual` b
            on a.var_establishment_id = b.establishment_id 
              or a.var_business_id = b.establishment_id 
              or a.var_establishment_id = b.account_id
              or a.var_business_id = b.account_id
          join base cb
            on b.establishment_id = cb.establishment_sfid
          where a.visit_date >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year) 
            and visit_date >= cb.creation_date 
            and visit_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          group by 1,2,3,4


)

        select distinct
          b.establishment_sfid as salesforce_id,
          b.country as country_code,
          ifnull(l.agg_portal_login, 0) as agg_login_portal,
          last_date_actual.last_portal_login_date as date_last_activity_portal_login,
          date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month

        from base b
        left join login_est l
          on b.establishment_sfid = l.establishment_sfid
        left join last_date_actual
        on b.establishment_sfid = last_date_actual.establishment_sfid      
    """,
        project,
    )

def query_as_absolute_mobile_app(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with

base as (

        select distinct 
          cb.establishment_sfid, 
          cb.SFDC_createdDate, 
          date(cb.SFDC_deletedDate) as SFDC_deletedDate, 
          date_add(date(cb.SFDC_createdDate), interval 7 day) creation_date,
          cb.country,
          case 
            when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) >= 12 then 12
            when date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month) = 0 then 1
            else date_diff(ifnull(if(date(cb.SFDC_deletedDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', date(cb.SFDC_deletedDate), null), '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'), date_add(date(cb.SFDC_createdDate), interval 7 day), month)
          end as n_12_month

        from  `{project}.refined.customer_base_establishment` cb
        where cb.SFDC_createdDate is not null
          and date(cb.SFDC_createdDate) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          and (date(cb.SFDC_deletedDate) >= '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}' or cb.SFDC_deletedDate is null) 

)

, adobe_mobile_app as (

          SELECT distinct
            traffic_establishment_id as establishment_sfid, 
            DATE(hit_date_time) as visit_date,
            max(DATE(hit_date_time)) over (partition by traffic_establishment_id) as last_mobile_app_login_date

          FROM {project}.refined.mobile_app_datafeed feed
          WHERE LOWER( CAST (mobileappid AS STRING)) NOT LIKE "%order%" 
            AND traffic_establishment_id IS NOT NULL
            AND valid_hit = 1
            AND DATE(hit_date_time) >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year) 
            AND DATE(hit_date_time) < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'


)

, last_date as (

        select  
          establishment_sfid,
          max(last_mobile_app_login_date) as last_mobile_app_login_date

        from adobe_mobile_app
        group by 1
)




, login_est as (

          select distinct 
            cb.establishment_sfid, 
            cb.country,
            n_12_month,
            count(distinct visit_date)/n_12_month as agg_mobile_login,
            

          from adobe_mobile_app a
          join base cb
            on a.establishment_sfid = cb.establishment_sfid
          where a.visit_date >= date_sub('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}', interval 1 year) 
            and visit_date >= cb.creation_date 
            and visit_date < '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'
          group by 1,2,3


)

        select distinct
          b.establishment_sfid as salesforce_id,
          b.country as country_code,
          ifnull(l.agg_mobile_login, 0) as agg_login_mobile_app,
          last_date.last_mobile_app_login_date as date_last_activity_mobile_login,
          date('{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month,

        from base b
        left join login_est l
          on b.establishment_sfid = l.establishment_sfid
        left join last_date
        on b.establishment_sfid = last_date.establishment_sfid         
    """,
        project,
    )

def query_as_absolute_activity_score(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """with 
      prep as (
                select  as_month,
                        salesforce_id, 
                        branch_office_id, 
                        country_code, 
                        wbv.agg_visitors AS wb_agg_visitors, --engagement
                        wbv.agg_login    AS wb_agg_logins,
                        wbe.agg_event    AS wb_agg_events,
                        rtl.agg_login_rt AS rt_agg_logins,
                        rte.agg_event    AS rt_agg_events,
                        rtr.agg_reservations AS rt_agg_reservations, --engagement
                        wl.agg_login_WL  AS wl_agg_logins,
                        mk.agg_login_MK  AS mk_agg_logins,
                        dol.agg_login_DO AS do_agg_logins,
                        dol.agg_event_DO AS do_agg_events,
                        doo.agg_order    AS do_agg_orders, --engagement
                        doo.agg_order_reacted_rel do_agg_reaction_rate,    

                        c.agg_Inbound    AS sfdc_agg_Inbound,
                        dco.agg_login_portal as portal_agg_logins,
                        da.agg_login_mobile_app as mobile_app_agg_logins,

                        'all' as country_type,
                        wbe.date_last_activity_wb_event,
                        rte.date_last_activity_rt_event, 
                        rtr.date_last_activity_reservation, 
                        dol.date_last_activity_do_event,
                        doo.date_last_activity_order,
                        mk.date_last_activity_mk_login,
                        c.date_last_inbound_call,
                        dco.date_last_activity_portal_login,
                        da.date_last_activity_mobile_login,
                        # Engagement Level for visitor
                        case 
                          when wbv.agg_visitors is null then null
                          else if(wbv.agg_visitors >= max(if(lower(th.KPI) = "visitor" and lower(th.user_type) = 'regular', th.Threshold, null)), 1, 0) 
                        end visitor_regular_user,

                        case 
                          when wbv.agg_visitors is null then null
                          else if(wbv.agg_visitors >= max(if(lower(th.KPI) = "visitor" and lower(th.user_type) = 'power', th.Threshold, null)), 1, 0) 
                        end visitor_power_user                  
            
                from `refined.as_absolute_wb_login_visitor` wbv 
                full join refined.as_absolute_wb_event wbe      
                using (salesforce_id, country_code, as_month)
                full join refined.as_absolute_rt_login rtl
                using (salesforce_id, country_code, as_month)          
                full join refined.as_absolute_rt_event rte
                using (salesforce_id, country_code, as_month)          
                full join refined.as_absolute_rt_reservation rtr
                using (salesforce_id, country_code, as_month)
                
                full join refined.as_absolute_wl_login wl
                using (salesforce_id, country_code, as_month)
                full join refined.as_absolute_do_login_event dol
                using (salesforce_id, country_code, as_month)
                
                full join refined.as_absolute_do_order doo
                using (salesforce_id, country_code, as_month)
                full join refined.as_absolute_mk_login mk
                using (salesforce_id, country_code, as_month)
                
                full join refined.as_absolute_sfdc_call c
                using (salesforce_id, country_code, as_month)

                full join refined.as_absolute_portal dco
                using (salesforce_id, country_code, as_month)

                full join refined.as_absolute_mobile_app da
                using (salesforce_id, country_code, as_month)

                left join (select * from `external.as_absolute_country_type` where valid_flag is true) ct
                using (country_code)
                left join (select * from `external.as_absolute_kpi_thresholds` where valid_flag is true) th 
                using (Country_Type)
                where as_month = '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}'  
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
      )
      , prep2 as (
                select distinct
                  if(has_POS = 1, cb.pos_vendor_id, null) pos_vendor_id, # take only active POS customers
                  ifnull(p.salesforce_id, if(has_POS = 1, cb.establishment_sfid, null)) as salesforce_id,
                  p.* except (salesforce_id,
                              country_code, 
                              country_type, 
                              visitor_regular_user, 
                              visitor_power_user, 
                              date_last_activity_wb_event, 
                              date_last_activity_rt_event, 
                              date_last_activity_reservation, 
                              date_last_activity_order,
                              date_last_activity_mk_login,
                              date_last_inbound_call,
                              date_last_activity_do_event,
                              date_last_activity_portal_login
                              ),                       

                case 
                  when p.wb_agg_logins is null then null
                  else if(p.wb_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'platform cms', th.threshold, null)), 1,0) 
                end activity_WB_login,

                case 
                  when p.wb_agg_events is null then null
                  else if(p.wb_agg_events >= max(if(lower(th.KPI) = 'event' and lower(th.tool) = 'platform cms', th.threshold, null)), 1,0) 
                end activity_WB_event,

                case 
                  when p.rt_agg_logins is null then null
                  else if(p.rt_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'reservation tool', th.threshold, null)), 1, 0) 
                end activity_RT_login,

                case 
                  when p.rt_agg_events is null then null
                  else if(p.rt_agg_events >= max(if(lower(th.KPI) = 'event' and lower(th.tool) = 'reservation tool', th.threshold, null)), 1,0) 
                end activity_RT_event,

                case 
                  when p.do_agg_logins is null then null
                  else if(p.do_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'order_tool', th.threshold, null)), 1, 0) 
                end activity_DO_login,

                case 
                  when p.do_agg_events is null then null
                  else if(p.do_agg_events >= max(if(lower(th.KPI) = 'event' and lower(th.tool) = 'order_tool', th.threshold, null)), 1,0) 
                end activity_DO_event,      

                case 
                  when p.do_agg_reaction_rate is null then null
                  else if(p.do_agg_reaction_rate >= max(if(lower(th.KPI) = 'reaction rate to orders (%)', th.threshold, null)),1,0) 
                end activity_DO_reaction,

                case 
                  when p.wl_agg_logins is null then null
                  else if(p.wl_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'web listing', th.threshold, null)), 1,0) 
                end activity_WL_login,

                case 
                  when p.mk_agg_logins is null then null
                  else if(p.mk_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'menu_kit', th.threshold, null)), 1,0) 
                end activity_MK_login,

                case 
                  when p.sfdc_agg_Inbound is null then null
                  else if(p.sfdc_agg_Inbound >= max(if(lower(th.KPI) = 'inbound call', th.threshold, null)), 1, 0) 
                end activity_SFDC_inbound,

                case 
                  when p.portal_agg_logins is null then null
                  else if(p.portal_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'portal', th.threshold, null)), 1,0) 
                end activity_PORTAL_login,

                case 
                  when p.mobile_app_agg_logins is null then null
                  else if(p.mobile_app_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'mobile app', th.threshold, null)), 1,0) 
                end activity_MOBILE_login,

                p.visitor_regular_user as engagement_visitor_regular,
                p.visitor_power_user as engagement_visitor_power,

                case 
                  when p.do_agg_orders is null then null
                  else if(p.do_agg_orders >= max(if(lower(th.KPI) = 'order' and lower(th.user_type) = 'regular', th.threshold, null)),1 ,0) 
                end engagement_order_regular,

                case 
                  when p.do_agg_orders is null then null
                  else if(p.do_agg_orders >= max(if(lower(th.KPI) = 'order' and lower(th.user_type) = 'power', th.threshold, null)),1 ,0) 
                end engagement_order_power,

                case 
                  when p.rt_agg_reservations is null then null
                  else if(p.rt_agg_reservations >= max(if(lower(th.KPI) = 'reservation' and lower(th.user_type) = 'regular', th.threshold, null)),1,0) 
                end engagement_reservation_regular,

                case 
                  when p.rt_agg_reservations is null then null
                  else if(p.rt_agg_reservations >= max(if(lower(th.KPI) = 'reservation' and lower(th.user_type) = 'power', th.threshold, null)),1,0) 
                end engagement_reservation_power,

                if(
                    p.wb_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'platform cms', th.threshold, null))
                    or
                    p.rt_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'reservation tool', th.threshold, null))
                    or 
                    p.do_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'order_tool', th.threshold, null))
                    or
                    p.wl_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'web listing', th.threshold, null))
                    or
                    p.mk_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'menu_kit', th.threshold, null))
                    or
                    p.portal_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'portal', th.threshold, null))
                    or
                    p.mobile_app_agg_logins >= max(if(lower(th.KPI) = 'login' and lower(th.tool) = 'mobile app', th.threshold, null))
                    or
                    p.wb_agg_events >= max(if(lower(th.KPI) = 'event' and lower(th.tool) = 'platform cms', th.threshold, null))
                    or 
                    p.rt_agg_events >= max(if(lower(th.KPI) = 'event' and lower(th.tool) = 'reservation tool', th.threshold, null))
                    or
                    p.do_agg_events >= max(if(lower(th.KPI) = 'event' and lower(th.tool) = 'order_tool', th.threshold, null))
                    or 
                    p.do_agg_reaction_rate >= max(if(lower(th.KPI) = 'reaction rate to orders (%)', th.threshold, null))
                    or p.sfdc_agg_Inbound >= max(if(lower(th.KPI) = 'inbound call', th.threshold, null))
                    , 1,0) activity,
                  greatest(
                            if(
                                p.do_agg_orders >= max(if(lower(th.KPI) = 'order' and lower(user_type) = 'regular', th.threshold, -1))
                                or
                                p.rt_agg_reservations >= max(if(lower(th.KPI) = 'reservation' and lower(th.user_type) = 'regular', th.threshold, -1)),1,0),
                            ifnull(p.visitor_regular_user,-1)
                          ) regular_user,
                  greatest(
                            if(
                                p.do_agg_orders >= max(if(lower(th.KPI) = 'order' and lower(th.user_type) = 'power', th.threshold, -1))
                                or
                                p.rt_agg_reservations >= max(if(lower(th.KPI) = 'reservation' and lower(th.user_type) = 'power', th.threshold, -1)),1,0),
                            ifnull(p.visitor_power_user, -1)
                          ) power_user,                

                  if (date(greatest(ifnull(cb.Platform_CMS_createdDate ,'1900-12-31'),
                                    ifnull(cb.Reservation_Tool_createdDate ,'1900-12-31'),
                                    ifnull(cb.Web_Listing_createdDate, '1900-12-31'), 
                                    ifnull(cb.Order_Tool_createdDate ,'1900-12-31'), 
                                    ifnull(cb.SFDC_createdDate ,'1900-12-31'),
                                    ifnull(cb.Menu_Kit_createdDate,'1900-12-31'),
                                    ifnull(cb.Legacy_POS_createdDate ,'1900-12-31'),
                                    ifnull(cb.POS_createdDate, '1900-12-31')))
                        between date_sub(p.as_month, interval 6 month)  
                            and p.as_month, 1, 0) new_est,
                  greatest( ifnull(p.date_last_activity_wb_event, '1900-01-01'), 
                            ifnull(p.date_last_activity_rt_event, '1900-01-01'), 
                            ifnull(p.date_last_activity_reservation, '1900-01-01'), 
                            ifnull(p.date_last_activity_order, '1900-01-01'),
                            ifnull(p.date_last_activity_do_event, '1900-01-01'),
                            ifnull(p.date_last_activity_mk_login, '1900-01-01'), 
                            ifnull(p.date_last_inbound_call, '1900-01-01'),
                            ifnull(adobe.date_last_activity_adobe, '1900-01-01'),
                            ifnull(p.date_last_activity_portal_login, '1900-01-01'),
                            ifnull(p.date_last_activity_mobile_login, '1900-01-01'))  date_last_activity,
                  ifnull(cb.country, max(p.country_code)) country_code,
                  if(cb.has_POS = 1, 1, null) has_POS
                from prep p
                left join (select * from `external.as_absolute_kpi_thresholds` where valid_flag is true) th
                on p.country_type = lower(th.country_type)
                full join `refined.customer_base_establishment` cb
                on salesforce_id = cb.establishment_sfid 
                left join (
                            select establishment_sfid, max(date_last_activity_adobe) date_last_activity_adobe
                            from refined.as_absolute_base_adobe
                            group by 1         
                          ) adobe
                on p.salesforce_id = adobe.establishment_sfid
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,
                        cb.country, 
                        visitor_regular_user,
                        visitor_power_user, 
                        Platform_CMS_createdDate, 
                        Reservation_Tool_createdDate, 
                        Web_Listing_createdDate, 
                        Order_Tool_createdDate, 
                        SFDC_createdDate, 
                        Menu_Kit_createdDate, 
                        Legacy_POS_createdDate, 
                        cb.POS_createdDate,
                        date_last_activity_wb_event, 
                        date_last_activity_rt_event, 
                        date_last_activity_reservation, 
                        date_last_activity_order,
                        date_last_activity_do_event,
                        date_last_activity_mk_login,
                        date_last_inbound_call,
                        date_last_activity_adobe,
                        date_last_activity_portal_login,
                        date_last_activity_mobile_login,
                        cb.has_POS
        )

          select 
            ifnull(as_month, '{{ next_execution_date.replace(day=1).strftime('%Y-%m-%d') }}') as as_month,
            salesforce_id, 
            branch_office_id, 
            pos_vendor_id,
            country_code,
            max( mobile_app_agg_logins) mobile_app_agg_logins,
            max( portal_agg_logins) portal_agg_logins,
            max( wb_agg_visitors) wb_agg_visitors,
            max( wb_agg_logins) wb_agg_logins,
            max( wb_agg_events) wb_agg_events,
            max( rt_agg_logins) rt_agg_logins,
            max( rt_agg_events) rt_agg_events,
            max( rt_agg_reservations) rt_agg_reservations,
            max( wl_agg_logins) wl_agg_logins,
            max( mk_agg_logins) mk_agg_logins,
            max( do_agg_logins) do_agg_logins,
            max( do_agg_events) do_agg_events,
            max( do_agg_orders) do_agg_orders,
            max( do_agg_reaction_rate) do_agg_reaction_rate,
            max( sfdc_agg_Inbound) sfdc_agg_Inbound,      
            max( activity_WB_login) engagement_WB_login,
            max( activity_WB_event) engagement_WB_event,
            max( activity_RT_login) engagement_RT_login,
            max( activity_RT_event) engagement_RT_event,
            max( activity_DO_login) engagement_DO_login,
            max( activity_DO_event) engagement_DO_event,
            max( activity_DO_reaction) engagement_DO_reaction,
            max( activity_WL_login) engagement_WL_login,
            max( activity_MK_login) engagement_MK_login,
            max( activity_MOBILE_login) as engagement_MOBILE_login,
            max( activity_PORTAL_login) as engagement_PORTAL_login,
            max( activity_SFDC_inbound) engagement_SFDC_inbound,       
            max( engagement_visitor_regular) activity_visitor_regular,
            max( engagement_visitor_power) activity_visitor_power,
            max( engagement_order_regular) activity_order_regular,
            max( engagement_order_power) activity_order_power,
            max( engagement_reservation_regular) activity_reservation_regular,
            max( engagement_reservation_power) activity_reservation_power,
            if(greatest(ifnull(max(activity_WB_login), -1), ifnull(max(activity_WB_event), -1)) = -1, null,
                greatest(ifnull(max(activity_WB_login), -1), ifnull(max(activity_WB_event), -1))
              ) + max(engagement_visitor_regular) + max(engagement_visitor_power) AS wb_activity_score,
              
            if(greatest(ifnull(max(activity_RT_login), -1), ifnull(max(activity_RT_event), -1)) = -1, null,
                greatest(ifnull(max(activity_RT_login), -1), ifnull(max(activity_RT_event), -1))
              ) + max(engagement_reservation_regular) + max(engagement_reservation_power) AS rt_activity_score,
              
            if(greatest(ifnull(max(activity_DO_login), -1), ifnull(max(activity_DO_event), -1), ifnull(max(activity_DO_reaction) , -1)) = -1, null,
                greatest(ifnull(max(activity_RT_login), -1), ifnull(max(activity_RT_event), -1), ifnull(max(activity_DO_reaction) , -1))
              ) + max(engagement_order_regular) + max(engagement_order_power) AS do_activity_score,
            max(has_POS) as pos_activity_score,
            max(activity) engagement, 
            max(if(regular_user = -1, null, regular_user)) regular_user, 
            max(if(power_user = -1, null, power_user)) power_user,
            (ifnull(max(regular_user),0) + ifnull(max(power_user),0) + ifnull(max(activity),0)) activity_score,
            max(new_est) new_est,
            if(
                max(if(regular_user = -1 ,0, regular_user)) + max(if(power_user = -1 ,0, power_user)) + ifnull(max(activity),0) > 0 
                or max(new_est) = 1

                or max(has_POS) = 1
                ,1,0) establishment_active,
            if(max(date_last_activity) = '1900-01-01', null, max(date_last_activity)) date_last_activity
          from prep2
          group by 1,2,3,4,5  
          
      """,
        project,
    )

def query_ce_activity_score_transitions(project: str = DEFAULT_PROJECT) -> str:
    return _fmt(
        """SELECT 
            salesforce_id,
            CASE WHEN activity_score1 = 1 AND activity_score2 = 0 THEN 1 ELSE 0 END AS has_downscored_1_0,
            CASE WHEN activity_score1 = 2 AND activity_score2 = 0 THEN 1 ELSE 0 END AS has_downscored_2_0,
            CASE WHEN activity_score1 = 3 AND activity_score2 = 0 THEN 1 ELSE 0 END AS has_downscored_3_0,
            CASE WHEN activity_score1 = 1 AND activity_score2 = 2 THEN 1 ELSE 0 END AS has_upscored_1_2,
            CASE WHEN activity_score1 = 1 AND activity_score2 = 3 THEN 1 ELSE 0 END AS has_upscored_1_3,
            CASE WHEN activity_score1 = 0 AND activity_score2 = 0 THEN 1 ELSE 0 END AS has_unchanged_0,
            CASE WHEN activity_score1 = 1 AND activity_score2 = 1 THEN 1 ELSE 0 END AS has_unchanged_1,
            CASE WHEN activity_score1 = 2 AND activity_score2 = 2 THEN 1 ELSE 0 END AS has_unchanged_2,
            CASE WHEN activity_score1 = 3 AND activity_score2 = 3 THEN 1 ELSE 0 END AS has_unchanged_3,
            CASE WHEN activity_score1 = 2 AND activity_score2 = 1 THEN 1 ELSE 0 END AS has_downscored_2_1,
            CASE WHEN activity_score1 = 3 AND activity_score2 = 1 THEN 1 ELSE 0 END AS has_downscored_3_1,
            CASE WHEN activity_score1 = 3 AND activity_score2 = 2 THEN 1 ELSE 0 END AS has_downscored_3_2,
            CASE WHEN activity_score1 = 2 AND activity_score2 = 3 THEN 1 ELSE 0 END AS has_upscored_2_3,
            CASE WHEN activity_score1 = 0 AND activity_score2 = 1 THEN 1 ELSE 0 END AS has_upscored_0_1,
            CASE WHEN activity_score1 = 0 AND activity_score2 = 2 THEN 1 ELSE 0 END AS has_upscored_0_2,
            CASE WHEN activity_score1 = 0 AND activity_score2 = 3 THEN 1 ELSE 0 END AS has_upscored_0_3,
          FROM (
            SELECT
              DISTINCT a.salesforce_id, a.activity_score activity_score1, IFNULL(b.activity_score,0) activity_score2
            FROM (
              SELECT DISTINCT salesforce_id, activity_score
              FROM `{project}.refined.as_absolute_activity_score`
              WHERE
                as_month = date_trunc (CURRENT_DATE(), month)) a
            LEFT JOIN (
              SELECT DISTINCT salesforce_id, activity_score
              FROM `{project}.refined.as_absolute_activity_score`
              WHERE as_month = DATE_TRUNC(CURRENT_DATE(), month) - INTERVAL 1 MONTH) b
            ON a.salesforce_id = b.salesforce_id)
        """,
        project,
    )

