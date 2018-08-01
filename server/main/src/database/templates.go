package database

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

var LOCATION_PREDICTION_SQL = `
	'{'||
	'"timestamp": ' ||  timestamp ||','||
	'"location": "' ||  locationid ||'",'||
	'"create_at": "' ||  create_at ||'",'||
	'"update_at": "' ||  update_at ||'",'||
	'"probability": ' ||  probability || '}'
`
