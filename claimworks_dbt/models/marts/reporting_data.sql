
with base as (
    select * from {{ ref('stg_insurance_data') }}
),

transformed as (
    select
        
        policy_id,
        customer_id,
        customer_name,
        case
            when lower(gender) in ('male', 'm') then 'M'
            when lower(gender) in ('female', 'f') then 'F'
            else 'O'
        end as gender,
        
        coalesce(age, date_part('year', current_date) - date_part('year', date_of_birth)) as age,
        case
            when age < 25 then '18-24'
            when age < 36 then '25-35'
            when age < 46 then '36-45'
            when age < 56 then '46-55'
            else '56+'
        end as age_group,
        premium_amount,
        -- Policy logic
        policy_type,
        lower(payment_frequency) as payment_frequency,
        case
            when payment_frequency = 'monthly' then premium_amount * 12
            when payment_frequency = 'quarterly' then premium_amount * 4
            when payment_frequency = 'yearly' then premium_amount
            else null
        end as annual_premium,

        policy_start_date,
        policy_end_date,
        policy_end_date - policy_start_date as policy_duration_days,
        now()::date - policy_start_date as policy_age_days,
        case
            when policy_end_date > current_date then true else false
        end as is_active,

        policy_status,
        claim_id,
        claim_amount,
        claim_status,
        claim_date,
        case
            when lower(claim_status) = 'approved' then 1 else 0
        end as has_approved_claim,
        current_date - claim_date as days_since_last_claim,

        -- Risk
        lower(risk_category) as risk_category,
        lower(smoker) = 'yes' as smoker_flag,
        health_condition,

        -- Assets
        vehicle_type,
        vehicle_age,
        case
            when vehicle_age < 5 then 'New'
            when vehicle_age < 10 then 'Mid'
            else 'Old'
        end as vehicle_age_group,

        property_type,
        property_value,
        case
            when property_value > 500000 then true else false
        end as is_high_value_property,
        marital_status,
        number_of_dependents,
        renewal_flag,
        last_payment_date,
        next_due_date,
        current_date - last_payment_date as days_since_last_payment,
        next_due_date - current_date as days_to_next_payment,
        case
            when next_due_date < current_date then true
            else false
        end as is_payment_due,
        email_address,
        zip_code,
        full_address,
        region

    from base
)

select * from transformed
