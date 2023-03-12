const getIsValidSql = (event_name) => {
    switch(event_name) {
        case "page_view": 
            return      `
                if(
                event_id is null
                or page_location is null
                or page_location = '(not set)'
                or NET.HOST(page_location) not in ('googlemerchandisestore.com', 'www.googlemerchandisestore.com','shop.googlemerchandisestore.com')
                ,false,true)`;
        case "purchase": 
            return `if(currency is not null,false,true)`;
    }
}

module.exports = {
  getIsValidSql,
};