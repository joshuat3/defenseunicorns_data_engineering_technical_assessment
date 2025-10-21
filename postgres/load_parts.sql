/*Load parts table*/
--streaming
insert into parts (manufacturer_id, part_no)
  select distinct rsd.manufacturer_id, rsd.part_number  
    from raw_streaming_data rsd left join parts p on rsd.manufacturer_id = p.manufacturer_id and rsd.part_number = p.part_no
    where 
          p.manufacturer_id is null and rsd.manufacturer_id is not null;

--batch
insert into parts (manufacturer_id, part_no)
    select distinct rbd.manufacturer_id, rbd.part_no  
    from raw_batch_data rbd left join parts p on rbd.manufacturer_id = p.manufacturer_id and rbd.part_no = p.part_no
    where 
          p.manufacturer_id is null and rbd.manufacturer_id is not null;