// Temporary tables
publish("page_view_tmp")
  .config({type: "table",schema: "dataform_staging"})
  .preOps(
    ctx => `
      DECLARE is_event_table DEFAULT (
      SELECT
         ANY_VALUE(table_type)
      FROM \`${helpers.getDatsetFromTableName(ctx.self())}.INFORMATION_SCHEMA.TABLES\`
      WHERE table_name = '${ctx.name()}'
     );
    `)
  .query(
    ctx => `
      with event_table as (
        SELECT
            FARM_FINGERPRINT(CONCAT(event_timestamp, event_name, user_pseudo_id)) as event_id,
            TIMESTAMP_MICROS(event_timestamp) as ts,
            DATE(TIMESTAMP_MICROS(event_timestamp)) as dt,
            CONCAT((SELECT ep.value.int_value AS ga_session_id FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id'), user_pseudo_id) as session_id,
            device.category as device_category,
            ${helpers.getEventParam('page_location')},
            ${helpers.getEventParam('page_referrer')},
            ${helpers.getEventParam('page_title')},
        FROM
            ${ctx.ref('events_*')}
        WHERE
        event_name = 'page_view'
        and REGEXP_CONTAINS(_TABLE_SUFFIX, if(is_event_table is null, r'.*', '${helpers.getDateFromTableName(constants.GA4_TABLE)}'))
        and contains_substr(_TABLE_SUFFIX, 'intraday') is not true

      )

      select
        *,
        ${validation_helpers.getIsValidSql("page_view")} as is_valid
      from event_table
`);

// Valid events
publish("page_view")
  .config(
          {
          type: "incremental",
          uniqueKey:['dt','event_id'],
          schema: "dataform_staging",
          bigquery: {
            partitionBy: "dt",
          }
        }
  ).query(
    ctx => `
      select
      * except (is_valid)
      from ${ctx.ref("page_view_tmp")} p
      where is_valid is true
    `
)

// Invalid events
publish("page_view_errors")
  .config(
          {
          type: "incremental",
          uniqueKey:['dt','event_id'],
          schema: "dataform_staging",
          bigquery: {
            partitionBy: "dt",
          }
        }
  ).query(
    ctx => `
      select
      * except (is_valid)
      from ${ctx.ref("page_view_tmp")} p
      where is_valid is not true
    `
)

// Intraday tables
publish("page_view_intraday")
  .config({type: "table",schema: "dataform_staging"})
  .query(
    ctx => `
    SELECT
      FARM_FINGERPRINT(CONCAT(event_timestamp, event_name, user_pseudo_id)) as event_id,
      TIMESTAMP_MICROS(event_timestamp) as ts,
      DATE(TIMESTAMP_MICROS(event_timestamp)) as dt,
      CONCAT((SELECT ep.value.int_value AS ga_session_id FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id'), user_pseudo_id) as session_id,
      device.category as device_category,
      ${helpers.getEventParam('page_location')},
      ${helpers.getEventParam('page_referrer')},
      ${helpers.getEventParam('page_title')},
      FROM
        ${ctx.ref('events_intraday_*')}
      WHERE
      event_name = 'page_view'
`);

// Intraday plus daily views
publish("page_view_view")
.config({type: "view",schema: "dataform_staging"})
.query(
  ctx => `
    select *, 'daily' as  event_type from ${ctx.ref("page_view")}
    union all
    select *, 'intraday' as  event_type from ${ctx.ref("page_view_intraday")}
  `
);
