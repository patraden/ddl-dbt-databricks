{% macro mt_deposits__comm_column_macro() %}
    CASE WHEN REGEXP_EXTRACT(lower(comment),'^([dw]-.*?)[-:\\\\s].*?') != ''
         THEN REGEXP_EXTRACT(lower(comment),'^([dw]-.*?)[-:\\\\s].*?')
         WHEN REGEXP_EXTRACT(lower(comment),'^(commission).*?') != ''
         THEN REGEXP_EXTRACT(lower(comment),'^(commission).*?')
         WHEN REGEXP_EXTRACT(lower(comment),'^(autoreff commission).*?') != ''
         THEN REGEXP_EXTRACT(lower(comment),'^(autoreff commission).*?')
         WHEN REGEXP_EXTRACT(lower(comment),'^(icc)[-:\\\\s].*?') != ''
         THEN REGEXP_EXTRACT(lower(comment),'^(icc)[-:\\\\s].*?')
         WHEN REGEXP_EXTRACT(lower(comment),'^(balance fixed).*?') != ''
         THEN REGEXP_EXTRACT(lower(comment),'^(balance fixed).*?')
         ELSE 'HZ'
    END comm
{% endmacro %}
