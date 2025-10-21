CREATE OR REPLACE VIEW legacy_data as
select 
    o.supplier_uuid order_uuid,
    c.component_name,
    c.system_name,
    p.manufacturer_id,
    p.part_no,
    o.serial_no,
    o.status,
    o.status_date,
    u.user_name ordered_by,
    o.comp_priority
from orders o 
inner join components c on o.component_id = c.component_id
inner join parts p on p.part_id = o.part_id
inner join users u on u.user_id = o.ordered_by
