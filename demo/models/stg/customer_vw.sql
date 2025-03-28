create or replace view stg.customer_vw as
(
    select
        id,
        trim(first_name) as fist_name,
        trim(last_name) as last_name,
        trim(email) as email,
        created_date
    from stg.customer
)
