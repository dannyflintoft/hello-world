drop table if exists #customer;
create temp table #customer (customer_id varchar(36));
insert into #customer
(select id from reporting.customer where name = 'Lovehoney');

drop table if exists #members;
create temp table #members (student_id varchar(36));
insert into #members(
            with joiners as(    
                        select member_id, p.partner_id, country_id, d.date_actual, j.event_type

                        from 
                                analytics.f_join_events je
                                inner join analytics.d_date d on d.date_key = je.date_key
                                inner join analytics.d_member m on m.member_key = je.member_key
                                inner join analytics.d_partner p on p.partner_key = je.partner_key
                                      and p.partner_id = (select * from #customer)
                                inner join analytics.d_join_event j on j.join_event_key = je.join_event_key
                                inner join analytics.d_country c on c.country_key = je.country_key
                                      and c.country_id = '880F6B70-0205-436E-BBCE-123E6BE2408C'  
            )      

            select member_id
            from(
                        select member_id
                               ,event_type
                               --,date_actual
                               ,row_number() over (partition by member_id order by date_actual desc) rank_
       
                        from joiners
            )
            where rank_ = 1
            and event_type = 'partner_follow'
);

select sum(post_count) impressions_mtd
FROM production.post_views pv
where pv.event in ('content card viewed','Content Card Viewed')
  and date_trunc('month',event_date)::date = date_trunc('month',current_date)::Date 
  and event_date != current_date::date
  and region_id = '76F07F34-7C78-4B47-ACB0-27A5B10A9F08'
  and partner_id = (select * from #customer)
;

select event_date::Date as date
       ,sum(post_count) impressions
FROM production.post_views pv
where pv.event in ('content card viewed','Content Card Viewed')
  and date_trunc('month',event_date)::date = date_trunc('month',current_date)::date 
  and region_id = '76F07F34-7C78-4B47-ACB0-27A5B10A9F08'
  and partner_id = (select * from #customer)
group by event_date::Date
order by 1
;

select case when feed_type = 'partner-feed' then 'Partner Feed'
            when feed_type = 'partner-thread' then 'Partner Thread'
            when feed_type = 'home-feed' then 'Home Feed'
        else feed_type end
        + case when m.student_id is null then ' - Non Member' else ' - Member' end as feed_type
       ,'Program Impressions'
       ,sum(post_count) impressions
from production.post_views pv
left join #members m on m.student_id = pv.student_id
where pv.event in ('content card viewed','Content Card Viewed')
  and event_date between current_date - 28 and current_date -1
  and region_id = '76F07F34-7C78-4B47-ACB0-27A5B10A9F08'
  and partner_id = (select * from #customer)
group by case when feed_type = 'partner-feed' then 'Partner Feed'
            when feed_type = 'partner-thread' then 'Partner Thread'
            when feed_type = 'home-feed' then 'Home Feed'
          else feed_type end
          + case when m.student_id is null then ' - Non Member' else ' - Member' end
order by 1

;
drop table if exists #customer;
drop table if exists #members;
