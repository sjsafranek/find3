sqlStmt = `
CREATE TABLE keystore (key text not null primary key, value text);
CREATE INDEX keystore_idx ON keystore(key);

CREATE TABLE sensors (timestamp integer not null primary key, deviceid text, locationid text, unique(timestamp));

CREATE TABLE location_predictions (timestamp integer NOT NULL PRIMARY KEY, prediction TEXT, UNIQUE(timestamp));

CREATE TABLE devices (id TEXT PRIMARY KEY, name TEXT);

CREATE TABLE locations (id TEXT PRIMARY KEY, name TEXT);

CREATE TABLE gps (id INTEGER PRIMARY KEY, timestamp INTEGER, mac TEXT, loc TEXT, lat REAL, lon REAL, alt REAL);

CREATE TABLE devices_name ON devices (name);
CREATE INDEX sensors_devices ON sensors (deviceid);
`

_, err = d.db.Exec(sqlStmt)
if err != nil {
    err = errors.Wrap(err, "MakeTables")
    logger.Log.Error(err)
    return
}
