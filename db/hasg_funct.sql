CREATE OR REPLACE FUNCTION ctr.fn_get_hash_row_v2(
    pk_flds text[],         -- Поля первичного ключа, на основе которых будет генерироваться хэш
    additional_flds text[], -- Дополнительные поля для генерации хэша
    schema_name text,       -- Имя схемы
    table_name text,        -- Имя таблицы
    alias_name text         -- Имя временной таблицы для использования в запросах
)
RETURNS TABLE (_json_res jsonb)
LANGUAGE plpgsql
AS $$
DECLARE
    query text;
    hash_value uuid;
    pk_json jsonb;
BEGIN
    -- Составляем запрос для получения первичных ключей и генерации хэша строки
    query := format('
        WITH cte AS (
            SELECT 
                %s as pk_flds, 
                md5(array_to_string(ARRAY[ %s ], ''||''))::uuid as hash_row
            FROM %I.%I
        )
        SELECT jsonb_build_object(
            ''pk_flds'', cte.pk_flds,
            ''hash_row'', cte.hash_row
        ) as _json_res
        FROM cte',
        array_to_string(pk_flds, ', '),
        array_to_string(pk_flds || additional_flds, ', '),
        schema_name,
        table_name
    );

    -- Выполняем динамический SQL-запрос и возвращаем JSON-результат
    RETURN QUERY EXECUTE query;
END;
$$;

select ctr.fn_get_hash_row_v2('{author_id}'::text[],'{hist_file_load_id}'::text[],'buffer','s_azs_rev_users','_yrbebj');
