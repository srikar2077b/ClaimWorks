
with source as (
    select * from {{ source('public', 'insurance_data') }}
),

cleaned as (
    select
        policy_id,
        customer_id,
        customer_name,
        lower(trim(gender)) as gender,
        date_of_birth,
        age,
        lower(policy_type) as policy_type,
        coverage_amount,
        premium_amount,
        lower(payment_frequency) as payment_frequency,
        policy_start_date,
        policy_end_date,
        claim_id,
        claim_date,
        claim_amount,
        lower(claim_reason) as claim_reason,
        lower(claim_status) as claim_status,
        agent_id,
        agent_name,
        lower(region) as region,
        customer_income,
        lower(marital_status) as marital_status,
        number_of_dependents,
        lower(vehicle_type) as vehicle_type,
        vehicle_age,
        lower(property_type) as property_type,
        property_value,
        lower(health_condition) as health_condition,
        lower(smoker) as smoker,
        lower(employment_status) as employment_status,
        lower(education_level) as education_level,
        lower(policy_status) as policy_status,
        last_payment_date,
        next_due_date,
        case
            when lower(renewal_flag) in ('yes', 'y', 'true') then true
            else false
        end as renewal_flag,
        lower(email_address) as email_address,
        trim(address) as address,
        zip_code,
        lower(risk_category) as risk_category,
        trim(full_address) as full_address
    from source
)

select * from cleaned
