-- models/marts/bridge/bridge_payload_customer.sql

/**
  Bridge table linking Payloads (dim_payload) to Customers (dim_customer) for the Many-to-Many relationship.
  Grain: One row per Payload-Customer association.
  Primary Key: Composite of payload_sk and customer_sk.
  Built by unnesting the customers list from stg_spacex_payloads and joining to dimensions.
  Uses the macro {{ split_string_list }} for Snowflake-specific unnesting.
*/
with stg_payloads as (

    -- Select Payload ID and the customers list from the payloads staging
    -- Filter out rows where the customer list is NULL or empty, as they don't have customer associations via this list.
    select
        payload_id, -- Necesitamos el ID natural del payload para unir a dim_payload
        customers_list
    from {{ ref('stg_spacex__payloads') }}
    where customers_list is not null and trim(customers_list) != ''

),

-- Desaplana la lista de clientes en filas, manteniendo el payload_id original.
-- Esto crea la base para las asociaciones (payload_id, customer_name).
unnested_payload_customers as (

    select
        sp.payload_id, -- Mantener el payload_id de la fila de staging
        -- La tabla generada por la macro (aliased como split_data) tiene una columna 'value'. Limpiar y castear.
        trim(split_data.value)::varchar as customer_name -- El nombre limpio del cliente para unir a dim_customer

    from stg_payloads sp, -- Le damos un alias 'sp' a la tabla staging
    {# *** Llama a la macro aquí en la cláusula FROM para generar la cláusula TABLE(...) Alias *** #}
    {{ split_string_list(column='sp.customers_list', alias='split_data', delimiter=',') }} -- Llama a la macro, pasando la columna (usando el alias 'sp.customers_list'), el alias deseado para la tabla de salida ('split_data') y el delimitador

    -- Filter out any results from the split that son nulos o cadenas vacías
    where split_data.value is not null and trim(split_data.value) != ''

),

dim_payload as (
    -- Referencia a dim_payload para obtener payload_sk usando el ID natural
    select payload_id, payload_sk from {{ ref('dim_payload') }}
),

dim_customer as (
    -- Referencia a dim_customer para obtener customer_sk usando el ID natural (nombre del cliente)
    select customer_id, customer_sk from {{ ref('dim_customer') }}
),

-- Obtener SKs de miembros desconocidos para COALESCE
-- Reutiliza las definiciones de miembro desconocido de las dimensiones.
unknown_payload_sk as ( select {{ dbt_utils.generate_surrogate_key(['\'unknown_payload\'']) }} as payload_sk ),
unknown_customer_sk as ( select {{ dbt_utils.generate_surrogate_key(['\'unknown_customer\'']) }} as customer_sk ),


final as (

    select
        -- Clave Foránea a dim_payload. Usa LEFT JOIN + COALESCE. Debería coincidir si payload_id existe en dim_payload.
        coalesce(dp.payload_sk, ups.payload_sk) as payload_sk,
        -- Clave Foránea a dim_customer. Usa LEFT JOIN + COALESCE. Debería coincidir si customer_name existe en dim_customer.
        coalesce(dc.customer_sk, ucs.customer_sk) as customer_sk,

        -- Métrica: Contar la asociación misma (siempre 1 por cada fila de la tabla puente)
        1 as count_of_payload_customer_associations

    from unnested_payload_customers npc
    -- Unir a dim_payload para obtener payload_sk. Usamos LEFT JOIN para robustez.
    LEFT JOIN dim_payload dp ON npc.payload_id = dp.payload_id
    -- Unir a dim_customer para obtener customer_sk. Usamos LEFT JOIN para robustez.
    -- La unión se hace sobre el nombre limpio del cliente (ID natural en dim_customer).
    LEFT JOIN dim_customer dc ON npc.customer_name = dc.customer_id -- Unir en el ID natural del cliente

    -- Cross join con los CTEs de miembros desconocidos para tener sus SKs disponibles para COALESCE
    cross join unknown_payload_sk ups
    cross join unknown_customer_sk ucs

    -- Opcional: Si solo quieres incluir asociaciones VÁLIDAS (donde tanto payload como cliente son conocidos),
    -- puedes añadir una cláusula WHERE aquí. Ejemplo:
    -- where coalesce(dp.payload_sk, ups.payload_sk) != ups.payload_sk -- Excluye asociaciones al miembro desconocido de payload
    --   and coalesce(dc.customer_sk, ucs.customer_sk) != ucs.customer_sk -- Excluye asociaciones al miembro desconocido de customer

)

-- Selección final para la tabla puente
select
    payload_sk,
    customer_sk,
    count_of_payload_customer_associations
from final
-- La clave primaria compuesta (payload_sk, customer_sk) se definirá en schema.yml