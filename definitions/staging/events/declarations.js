events_config.GA4_EVENTS.forEach(eventConfig => {
  const eventName = eventConfig.eventName;
  const eventTbl = eventConfig.eventName;
  const eventTempTbl = `${eventConfig.eventName}_tmp`;
  const eventErrorsTbl = `${eventConfig.eventName}_errors`;
  const eventIntradayTbl = `${eventConfig.eventName}_intraday`;
  const eventView = `${eventConfig.eventName}_view`;

  // Temporary tables
  publish(eventTempTbl)
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
              ${helpers.getTableColumns(eventConfig.params)},
              ${helpers.getTableColumnsUnnestEventParameters(eventConfig.eventParams)},
          FROM
              ${ctx.ref('events_*')}
          WHERE
          event_name = '${eventName}'
          and REGEXP_CONTAINS(_TABLE_SUFFIX, if(is_event_table is null, r'.*', '${helpers.getDateFromTableName(constants.GA4_TABLE)}'))
          and contains_substr(_TABLE_SUFFIX, 'intraday') is not true

        )

        select
          *,
          ${validation_helpers.getIsValidSql(eventName)} as is_valid
        from event_table
  `);

  // Valid events
  publish(eventTbl)
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
        from ${ctx.ref(eventTempTbl)} p
        where is_valid is true
      `
  )

  // Invalid events
  publish(eventErrorsTbl)
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
        from ${ctx.ref(eventTempTbl)} p
        where is_valid is not true
      `
  )

  // Intraday tables
  publish(eventIntradayTbl)
    .config({type: "table",schema: "dataform_staging"})
    .query(
      ctx => `
      SELECT
        FARM_FINGERPRINT(CONCAT(event_timestamp, event_name, user_pseudo_id)) as event_id,
        TIMESTAMP_MICROS(event_timestamp) as ts,
        DATE(TIMESTAMP_MICROS(event_timestamp)) as dt,
        CONCAT((SELECT ep.value.int_value AS ga_session_id FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id'), user_pseudo_id) as session_id,
        ${helpers.getTableColumns(eventConfig.params)},
        ${helpers.getTableColumnsUnnestEventParameters(eventConfig.eventParams)},
        FROM
          ${ctx.ref('events_intraday_*')}
        WHERE
        event_name = '${eventName}'
  `);

  // Intraday plus daily views
  publish(eventView)
  .config({type: "view",schema: "dataform_staging"})
  .query(
    ctx => `
      select *, 'daily' as  event_type from ${ctx.ref(eventTbl)}
      union all
      select *, 'intraday' as  event_type from ${ctx.ref(eventIntradayTbl)}
    `
  );

});

