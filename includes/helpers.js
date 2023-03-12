const getEventParam = (eventParamName, eventParamType = "string", columnName = false) => {
  let eventParamTypeName = "";
  switch (eventParamType) {
    case "string":
      eventParamTypeName = "string_value";
      break;
    case "int":
      eventParamTypeName = "int_value";
      break;
    case "double":
      eventParamTypeName = "double_value";
      break;
    case "float":
      eventParamTypeName = "float_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT ep.value.${eventParamTypeName} AS ${eventParamName} FROM UNNEST(event_params) ep WHERE ep.key = '${eventParamName}') AS ${
    columnName ? columnName : eventParamName
  }`;
};

const getDateFromTableName = (tblName) =>{
  return tblName.substring(7);
}
const getDatsetFromTableName = (tblName) =>{
  return tblName.substring(1,tblName.lastIndexOf('.'))

}
module.exports = {
  getEventParam,
  getDatsetFromTableName,
  getDateFromTableName
};