package database

// SENSOR_SQL is the sql json template for a sensor object.
var SENSOR_SQL = `
	'{'||
	'"timestamp": ' ||  timestamp ||','||
	'"deviceid": "' ||  deviceid ||'",'||
	'"locationid": "' ||  locationid ||'",'||
	'"create_at": "' ||  create_at ||'",'||
	'"update_at": "' ||  update_at ||'",'||
	'"sensor_type": "' ||  sensor_type ||'",'||
	'"sensor": ' ||  sensor || '}'
`

// LOCATION_PREDICTION_SQL is the sql json template for
// a location_prediction object.
var LOCATION_PREDICTION_SQL = `
	'{'||
	'"timestamp": ' ||  timestamp ||','||
	'"location": "' ||  locationid ||'",'||
	'"create_at": "' ||  create_at ||'",'||
	'"update_at": "' ||  update_at ||'",'||
	'"probability": ' ||  probability || '}'
`
