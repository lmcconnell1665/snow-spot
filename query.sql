select
  DATEADD('DAY', 7, CURRENT_TIMESTAMP()) as due_date, -- hs_timestamp
  CONCAT(
    'Send a scorecard for the ',
    count(o.job_order_id),
    ' roles open with ',
    c.company_name
  ) as task_description, --hs_task_body
  e.hubspot_owner_id as task_owner_id, --hubspot_owner_id
  'Send Customer Scorecard' as task_title, --hs_task_subject
  'NOT_STARTED' as task_status, --hs_task_status
  'MEDIUM' as task_priority, --hs_task_priority
  'EMAIL' as task_type, --hs_task_type
  c.company_id as hubspot_company_id,
  c.company_name,
  count(o.job_order_id) as open_job_orders,
  c.record_owner_name
from operations.dim_job_orders o
left join masterdata.companies c
    on o.customer_id = c.talent_studio_id
left join masterdata.employees e
    on c.record_owner_email = e.email
where 
    o.is_open = TRUE
    and c.company_id is not null
    and o.business_unit in ('Talent Solutions', 'Technology Solutions')
group by
  c.company_id,
  c.company_name,
  c.record_owner_name,
  c.record_owner_email,
  e.hubspot_owner_id
order by open_job_orders desc
limit 1