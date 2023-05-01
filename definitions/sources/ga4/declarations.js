declare({
  database: constants.GA4_DATABASE,
  schema: dataform.projectConfig.vars.GA4_DATASET,
  name: dataform.projectConfig.vars.GA4_TABLE,
});

declare({
  database: constants.GA4_DATABASE,
  schema: dataform.projectConfig.vars.GA4_DATASET,
  name: 'events_*',
});

declare({
  database: constants.GA4_DATABASE,
  schema: dataform.projectConfig.vars.GA4_DATASET,
  name: 'events_intraday_*',
});