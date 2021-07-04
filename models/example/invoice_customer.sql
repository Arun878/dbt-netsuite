
with invoice_data as(
    select * from {{ source("public", "invoice") }}
),
customer_data as(
    select * from {{ source("public", "customer") }}
),
currency_data as(
    select * from {{ source("public", "currency") }}
)

select i.id, 
    c.entityid as name,
    CAST(i.createddate as date) as created_date,
    CAST(i.duedate as date) as due_date,
    CAST(i.lastmodifieddate as date) as last_modified_dat,
    i.typebaseddocumentnumber as document_number,
    d.name as currency,
    i.foreigntotal as amount_foreign,
    i.posting as posting,
    i.void as void,
    i.ordpicked as order_picked,
    i.estgrossprofit as est_gross_profit,
    i.daysopen as days_open,
    i.billingstatus as billing_status,
    i.isreversal as is_reversal,
    case
        when i.status = 'A'
            then 'Open'
        else 'Paid in Full'
    end as status
from invoice_data i 
left join customer_data c on i.entity=c.id
left join currency_data d on i.currency=d.id

