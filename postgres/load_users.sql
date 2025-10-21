/* Load user table*/
--streaming
insert into users (user_name)
select 
   distinct 
      case when position(',' in rsd.ordered_by) > 0 then lower(substring(rsd.ordered_by,position(',' in rsd.ordered_by)+2,length(rsd.ordered_by)-position(',' in rsd.ordered_by)+2)) ||'.' || lower(left(rsd.ordered_by,position(',' in rsd.ordered_by)-1))
        else 
       lower(replace(replace(rsd.ordered_by,', ','.'),' ','.')) 
    end ordered_by_update
from raw_streaming_data rsd 
left join users u on case when position(',' in rsd.ordered_by) > 0 then lower(substring(rsd.ordered_by,position(',' in rsd.ordered_by)+2,length(rsd.ordered_by)-position(',' in rsd.ordered_by)+2)) ||'.' || lower(left(rsd.ordered_by,position(',' in rsd.ordered_by)-1))
        else 
       lower(replace(replace(rsd.ordered_by,', ','.'),' ','.')) 
    end = u.user_name
where 
  u.user_name is null and 
  case 
     when position(',' in rsd.ordered_by) > 0 then lower(substring(rsd.ordered_by,position(',' in rsd.ordered_by)+2,length(rsd.ordered_by)-position(',' in rsd.ordered_by)+2)) ||'.' || lower(left(rsd.ordered_by,position(',' in rsd.ordered_by)-1))
  else 
     lower(replace(replace(rsd.ordered_by,', ','.'),' ','.')) 
  end  is not null;

--batch
insert into users (user_name)
select 
   distinct 
      case when position(',' in rbd.ordered_by) > 0 then lower(substring(rbd.ordered_by,position(',' in rbd.ordered_by)+2,length(rbd.ordered_by)-position(',' in rbd.ordered_by)+2)) ||'.' || lower(left(rbd.ordered_by,position(',' in rbd.ordered_by)-1))
        else 
       lower(replace(replace(rbd.ordered_by,', ','.'),' ','.')) 
    end ordered_by_update
from raw_batch_data rbd 
left join users u on case when position(',' in rbd.ordered_by) > 0 then lower(substring(rbd.ordered_by,position(',' in rbd.ordered_by)+2,length(rbd.ordered_by)-position(',' in rbd.ordered_by)+2)) ||'.' || lower(left(rbd.ordered_by,position(',' in rbd.ordered_by)-1))
        else 
       lower(replace(replace(rbd.ordered_by,', ','.'),' ','.')) 
    end = u.user_name
where 
  u.user_name is null and 
  case 
     when position(',' in rbd.ordered_by) > 0 then lower(substring(rbd.ordered_by,position(',' in rbd.ordered_by)+2,length(rbd.ordered_by)-position(',' in rbd.ordered_by)+2)) ||'.' || lower(left(rbd.ordered_by,position(',' in rbd.ordered_by)-1))
  else 
     lower(replace(replace(rbd.ordered_by,', ','.'),' ','.')) 
  end  is not null;