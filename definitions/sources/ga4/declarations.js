declare({
  database: constants.GA4_DATABASE,
  schema: constants.GA4_DATASET,
  name: constants.GA4_TABLE,
});

declare({
  database: constants.GA4_DATABASE,
  schema: constants.GA4_DATASET,
  name: 'events_*',
});

declare({
  database: constants.GA4_DATABASE,
  schema: constants.GA4_DATASET,
  name: 'events_intraday_*',
});