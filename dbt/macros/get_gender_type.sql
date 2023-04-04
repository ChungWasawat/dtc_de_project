{#
    This macro returns the description of the payment_type
#}

{% macro get_gender_type(payment_type) -%}

    case {{ payment_type }}
        when 0 then 'Unknown'
        when 1 then 'Male'
        when 2 then 'Female'

    end

{%- endmacro %}