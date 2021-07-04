
with bills_data as(    
    select * from {{ source("public", "bills") }}
),
vendor_data as(
    select * from {{ source("public", "vendor") }}
),
currency_data as(
    select * from {{ source("public", "currency") }}
)

select b.entity, 
    v.entityid,
    b.duedate,
    d.name as curreny,
    CAST(b.duedate as date) as daaaara

from bills_data b 
left join vendor_data v on b.entity=v.id
left join currency_data d on b.currency=d.id