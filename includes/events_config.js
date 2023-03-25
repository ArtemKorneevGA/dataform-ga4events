const GA4_EVENTS = [
    {
        eventName: 'page_view',
        eventParams: [
            { name: "page_location", type: "string" },
            { name: "page_referrer", type: "string" },
            { name: "page_title", type: "string" },
        ],
        params: [
            { name: "device.category", columnName: "device_category" },
        ],
    },
    {
        eventName: 'purchase',
        eventParams: [
            { name: "page_location", type: "string" },
            { name: "page_referrer", type: "string" },
            { name: "currency", type: "string" },
            { name: "coupon", type: "string" },
        ],
        params: [
            { name: "device.category", columnName: "device_category" },
        ],
    },
 ];
 module.exports = { GA4_EVENTS };