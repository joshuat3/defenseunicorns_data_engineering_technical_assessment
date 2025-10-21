/*Load Orders*/
--batch
with orders as (
    select 
       distinct 
       order_uuid, 
       manufacturer_id, 
       serial_no, 
       part_no, 
       lower(replace(replace(component_name,' ','_'),'-','_')) component_name 
    from raw_batch_data 
    where status <> 'RECEIVED'
),
order_dates as (
   select 
    order_uuid,
    status_date ordered_date
   from raw_batch_data 
   where status = 'ORDERED'
),
ordered_by_list_stage as (
    select 
        distinct 
        order_uuid, 
        case when position(',' in ordered_by) > 0 then lower(substring(ordered_by,position(',' in ordered_by)+2,length(ordered_by)-position(',' in ordered_by)+2)) ||'.' || lower(left(ordered_by,position(',' in ordered_by)-1))
        else 
       lower(replace(replace(ordered_by,', ','.'),' ','.')) 
    end ordered_by
    from raw_batch_data rbd 
    where ordered_by is not null and status <> 'RECEIVED'
),
ordered_by_list as (
    select 
        ls.order_uuid,
        ls.ordered_by,
        u.user_id
    from ordered_by_list_stage ls inner join users u on ls.ordered_by = u.user_name     
),
max_status_date as (
    select 
       max(status_date) last_status_date,
       order_uuid
    from raw_batch_data
    group by order_uuid
),
last_status as (
    select 
        s.order_uuid,
        s.status last_status,
        last_status_date
    from raw_batch_data s inner join max_status_date msd on s.order_uuid = msd.order_uuid and s.status_date = msd.last_status_date
)

insert into orders (
    supplier_uuid,
    component_id,
    part_id,
    serial_no,
    comp_priority,
    order_date,
    ordered_by,
    status,
    status_date
)
 select 
    o.order_uuid,
    c.component_id,
    p.part_id,
    o.serial_no,
    false,
    od.ordered_date,
    obl.user_id,
    ls.last_status status,
    ls.last_status_date status_date
    
from orders o 
    left join parts p on o.part_no = p.part_no and o.manufacturer_id = p.manufacturer_id
    left join components c on c.component_name = o.component_name
    left join order_dates od on o.order_uuid = od.order_uuid
    left join ordered_by_list obl on o.order_uuid = obl.order_uuid
    left join last_status ls on o.order_uuid = ls.order_uuid;


--streaming
with orders as (
    select distinct order_uuid, manufacturer_id, serial_number, part_number, lower(replace(replace(component_name,' ','_'),'-','_')) component_name from raw_streaming_data where status <> 'RECEIVED'
    and part_number is not null
),
order_dates as (
   select 
    order_uuid,
    cast(datetime as timestamp) ordered_date
   from raw_streaming_data where status = 'ORDERED'
),
ordered_by_list_stage as (
    select 
        distinct 
        order_uuid, 
        case when position(',' in ordered_by) > 0 then lower(substring(ordered_by,position(',' in ordered_by)+2,length(ordered_by)-position(',' in ordered_by)+2)) ||'.' || lower(left(ordered_by,position(',' in ordered_by)-1))
        else 
       lower(replace(replace(ordered_by,', ','.'),' ','.')) 
    end ordered_by
    from raw_streaming_data rbd 
    where ordered_by is not null and status <> 'RECEIVED'
),
ordered_by_list as (
    select 
        ls.order_uuid,
        ls.ordered_by,
        u.user_id
    from ordered_by_list_stage ls inner join users u on ls.ordered_by = u.user_name     
),
max_status_date as (
    select 
       max(cast(datetime as timestamp)) last_status_date,
       order_uuid
    from raw_streaming_data
    group by order_uuid
),
last_status as (
    select 
        s.order_uuid,
        s.status last_status,
        last_status_date
    from raw_streaming_data s 
    inner join max_status_date msd on s.order_uuid = msd.order_uuid and cast(s.datetime as timestamp) = msd.last_status_date
)

insert into orders (
    supplier_uuid,
    component_id,
    part_id,
    serial_no,
    comp_priority,
    order_date,
    ordered_by,
    status,
    status_date
)

 select 
    o.order_uuid,
    c.component_id,
    p.part_id,
    o.serial_number,
    true,
    od.ordered_date,
    obl.user_id,
    ls.last_status status,
    ls.last_status_date status_date
    
from orders o 
    left join parts p on o.part_number = p.part_no and o.manufacturer_id = p.manufacturer_id
    left join components c on c.component_name = o.component_name
    left join order_dates od on o.order_uuid = od.order_uuid
    left join ordered_by_list obl on o.order_uuid = obl.order_uuid
    left join last_status ls on o.order_uuid = ls.order_uuid;



