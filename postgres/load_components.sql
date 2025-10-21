/*Load components table*/
--steraming
insert into components (component_name, system_name)
  select distinct 
    lower(replace(replace(rsd.component_name,' ','_'),'-','_')) component_name, 
    rsd.system_name 
  from raw_streaming_data rsd 
  left join components c on lower(replace(replace(rsd.component_name,' ','_'),'-','_')) = c.component_name 
  where c.component_name is null and rsd.component_name is not null;
 
--batch
insert into components (component_name, system_name)
  select distinct 
    lower(replace(replace(rbd.component_name,' ','_'),'-','_')) component_name, 
    rbd.system_name 
  from raw_batch_data rbd 
  left join components c on lower(replace(replace(rbd.component_name,' ','_'),'-','_')) = c.component_name 
  where c.component_name is null and rbd.component_name is not null;