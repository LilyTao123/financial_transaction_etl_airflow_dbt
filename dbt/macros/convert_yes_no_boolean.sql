{#
    This macro returns the True or False 
#}

{% macro convert_yes_no_to_boolean(column_name) -%}
    CASE 
        WHEN LOWER({{column_name }}) = 'yes' THEN TRUE
        WHEN LOWER({{column_name }}) = 'no' THEN FALSE
        ELSE NULL  -- Handle unexpected values
    END

{%- endmacro %}