{% macro normalize_company_name(col) -%}
trim(regexp_replace(
  regexp_replace(
    regexp_replace(upper({{ col }}), '[^A-Z0-9 ]', '', 'g'),
    '\m(LLC|INC|LP|LLP|LTD|CO|CORP|CORPORATION|COMPANY)\M', '', 'g'),
  '\s+', ' ', 'g'))
{%- endmacro %}
