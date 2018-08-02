package database

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/md5"
	"encoding/hex"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/schollz/find4/server/main/src/models"
	"github.com/schollz/sqlite3dump"
	// "github.com/schollz/stringsizer"
	// "github.com/satori/go.uuid"
)

// MakeTables creates two tables, a `keystore` table:
//
// 	KEY (TEXT)	VALUE (TEXT)
//
// and also a `sensors` table for the sensor data:
//
// 	TIMESTAMP (INTEGER)	DEVICE(TEXT) LOCATION(TEXT)
//
// the sensor table will dynamically create more columns as new types
// of sensor data are inserted. The LOCATION column is optional and
// only used for learning/classification.
func (self *Database) MakeTables() (err error) {
	logger.Log.Debug("create database tables for %v", self.family)
	_, err = self.db.Exec(TABLES_SQL)
	if err != nil {
		logger.Log.Error(err)
		Fatal(err, "failed to create database tables")
		return
	}
	return
}

func (self *Database) PrepareQuery(query string) (*sql.Stmt, error) {
	logger.Log.Trace(query)
	stmt, err := self.db.Prepare(query)
	if err != nil {
		panic(err)
		err = errors.Wrap(err, "problem preparing SQL")
	}
	return stmt, err
}

func (self *Database) runQuery(stmt *sql.Stmt) (*sql.Rows, error) {
	rows, err := stmt.Query()
	if err != nil {
		panic(err)
		err = errors.Wrap(err, "problem executing query")
	}
	return rows, err
}

func (self *Database) prepareAndRunQueryWithCallback(query string, clbk func(*sql.Rows) error) error {
	stmt, err := self.PrepareQuery(query)
	if nil != err {
		return err
	}
	defer stmt.Close()
	rows, err := self.runQuery(stmt)
	if nil != err {
		return err
	}
	defer rows.Close()
	return clbk(rows)
}

func (self *Database) runQuerySync(clbk func(string)) {
	var wg sync.WaitGroup
	wg.Add(1)
	self.requestQueue <- func(query_id string) {
		defer wg.Done()
		clbk(query_id)
	}
	wg.Wait()
}

func (self *Database) runQueryAsync(clbk func(string)) {
	self.requestQueue <- func(query_id string) {
		clbk(query_id)
	}
}

func (self *Database) runReadQuery(clbk func(string, *Database)) {
	query_id := self.getQId("r")

	// open database for reading
	reader, err := Open(self.family)
	if nil != err {
		Fatal(err, "Could not open database")
	}
	defer reader.Close()

	// run callback
	clbk(query_id, reader)
}

// Get will retrieve the value associated with a key.
// channel event listener makes database call synchronous
func (self *Database) Get(key string, v interface{}) error {
	var err error
	self.runReadQuery(func(query_id string, db *Database) {
		stmt, err := db.PrepareQuery("SELECT value FROM keystore WHERE key = ?")
		if nil != err {
			return
		}
		defer stmt.Close()
		var result string
		err = stmt.QueryRow(key).Scan(&result)
		if err != nil {
			err = errors.Wrap(err, "problem getting key")
			return
		}
		err = json.Unmarshal([]byte(result), &v)
	})
	return err
}

// Set will set a value in the database, when using it like a keystore.
func (self *Database) Set(key string, value interface{}) (err error) {
	var b []byte
	b, err = json.Marshal(value)
	if err != nil {
		return err
	}

	self.runQueryAsync(func(query_id string) {
		tx, err := self.db.Begin()
		if err != nil {
			Fatal(err, "Set Begin")
		}

		stmt, err := tx.Prepare("INSERT OR REPLACE INTO keystore(key,value) VALUES (?, ?)")
		if err != nil {
			Fatal(err, "Set Prepare")
		}
		defer stmt.Close()

		_, err = stmt.Exec(key, string(b))
		if err != nil {
			Fatal(err, "Set Execute")
		}

		err = tx.Commit()
		if err != nil {
			Fatal(err, "Set Commit")
		}
	})

	return
}

// Dump will output the string version of the database
func (self *Database) Dump() (dumped string, err error) {
	var b bytes.Buffer
	out := bufio.NewWriter(&b)
	err = sqlite3dump.Dump(self.name, out)
	if err != nil {
		return
	}
	out.Flush()
	dumped = string(b.Bytes())
	return
}

// AddPrediction will insert or update a prediction in the database
func (self *Database) AddPrediction(timestamp int64, aidata []models.LocationPrediction) (err error) {
	// make sure we have a prediction
	if len(aidata) == 0 {
		err = errors.New("no predictions to add")
		return
	}

	self.runQueryAsync(func(query_id string) {
		tx, err := self.db.Begin()
		if err != nil {
			Fatal(err, "AddPrediction Begin")
		}

		query := "INSERT OR REPLACE INTO location_predictions (timestamp, locationid, probability) VALUES (?, ?, ?)"
		logger.Log.Trace(query)
		stmt, err := tx.Prepare(query)
		if err != nil {
			Fatal(err, "AddPrediction Prepare")
		}
		defer stmt.Close()

		// truncate to two digits
		for i := range aidata {
			aidata[i].Probability = float64(int64(float64(aidata[i].Probability)*100)) / 100
			_, err = stmt.Exec(timestamp, aidata[i].Location, aidata[i].Probability)
			if err != nil {
				Fatal(err, "AddPrediction Execute")
			}
		}

		err = tx.Commit()
		if err != nil {
			Fatal(err, "AddPrediction Commit")
		}
	})

	return
}

// GetPrediction will retrieve models.LocationAnalysis associated with that timestamp
func (self *Database) GetPrediction(timestamp int64) (aidata []models.LocationPrediction, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		stmt, err := db.PrepareQuery(`
		SELECT '[' ||
			(SELECT IFNULL(GROUP_CONCAT(prediction), '') FROM (
				SELECT ` + LOCATION_PREDICTION_SQL + ` AS prediction
			 	FROM location_predictions WHERE timestamp = ?
			))
		|| ']'`)

		if err != nil {
			return
		}
		defer stmt.Close()
		var result string
		err = stmt.QueryRow(timestamp).Scan(&result)
		if err != nil {
			err = errors.Wrap(err, "problem getting key")
			return
		}

		err = json.Unmarshal([]byte(result), &aidata)
		if err != nil {
			return
		}
	})
	return
}

// AddSensor will insert a sensor data into the database
// TODO: AddSensor should be special case of AddSensors
func (self *Database) AddSensor(s models.SensorData) (err error) {
	self.runQueryAsync(func(query_id string) {
		// setup the database
		tx, err := self.db.Begin()
		if err != nil {
			Fatal(err, "AddSensor Begin")
		}

		device_id := s.Device
		location_id := ""
		if len(s.Location) > 0 {
			location_id = s.Location
		}

		sqlStatement := "INSERT OR REPLACE INTO sensors(timestamp, deviceid, locationid, sensor_type, sensor) VALUES (?, ?, ?, ?, ?)"
		stmt, err := tx.Prepare(sqlStatement)
		if err != nil {
			Fatal(err, "AddSensor Prepare")
		}
		defer stmt.Close()

		for sensor_type, sensor := range s.Sensors {
			data, _ := json.Marshal(sensor)
			_, err = stmt.Exec(s.Timestamp, device_id, location_id, sensor_type, string(data))
			if err != nil {
				Fatal(err, "AddSensor Execute")
			}
		}

		err = tx.Commit()
		if err != nil {
			Fatal(err, "AddSensor Commit")
		}
	})
	return
}

// GetSensorFromTime will return a sensor data for a given timestamp
func (self *Database) GetSensorFromTime(timestamp interface{}) (s models.SensorData, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		sensors, err := db.GetAllFromPreparedQuery("SELECT "+SENSOR_SQL+" FROM sensors WHERE timestamp = ?", timestamp)
		if err != nil {
			err = errors.Wrap(err, "GetSensorFromTime")
		}
		s = sensors[0]
	})
	return
}

// Get will retrieve the value associated with a key.
func (self *Database) GetLastSensorTimestamp() (timestamp int64, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		stmt, err := db.PrepareQuery("SELECT timestamp FROM sensors ORDER BY timestamp DESC LIMIT 1")
		if nil != err {
			return
		}
		defer stmt.Close()
		err = stmt.QueryRow().Scan(&timestamp)
		if err != nil {
			err = errors.Wrap(err, "problem getting key")
		}
	})
	return
}

// Get will retrieve the value associated with a key.
func (self *Database) TotalLearnedCount() (count int64, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		stmt, err := db.PrepareQuery("SELECT count(timestamp) FROM sensors WHERE locationid != ''")
		if err != nil {
			return
		}

		defer stmt.Close()
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			err = errors.Wrap(err, "problem getting key")
		}
	})
	return
}

// TODO
//  - single transaction
// GetSensorFromGreaterTime will return a sensor data for a given timeframe
func (self *Database) GetSensorFromGreaterTime(timeBlockInMilliseconds int64) (sensors []models.SensorData, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		latestTime, err := db.GetLastSensorTimestamp()
		if err != nil {
			return
		}
		minimumTimestamp := latestTime - timeBlockInMilliseconds
		sensors, err = db.GetAllFromPreparedQuery("SELECT "+SENSOR_SQL+" FROM (SELECT * FROM sensors WHERE timestamp > ? GROUP BY deviceid ORDER BY timestamp DESC)", minimumTimestamp)
	})
	return
}

func (self *Database) NumDevices() (num int, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		stmt, err := db.PrepareQuery("SELECT COUNT(DISTINCT deviceid) FROM sensors WHERE deviceid != ''")
		if err != nil {
			return
		}

		defer stmt.Close()
		err = stmt.QueryRow().Scan(&num)
		if err != nil {
			err = errors.Wrap(err, "problem getting key")
		}
	})
	return
}

func (self *Database) GetDeviceFirstTimeFromDevices(devices []string) (firstTime map[string]time.Time, err error) {
	firstTime = make(map[string]time.Time)

	if 0 == len(devices) {
		return
	}

	self.runReadQuery(func(query_id string, db *Database) {
		err = db.prepareAndRunQueryWithCallback(fmt.Sprintf(`
	SELECT n,t FROM (
		SELECT
			deviceid AS n,
			timestamp AS t
		FROM sensors
		WHERE deviceid IN ('%s')
		ORDER BY timestamp desc
	)
	GROUP BY n`, strings.Join(devices, "','")),
			func(rows *sql.Rows) error {
				for rows.Next() {
					var name string
					var ts int64
					err = rows.Scan(&name, &ts)
					if err != nil {
						return errors.Wrap(err, "error while scanning row")
					}

					firstTime[name] = time.Unix(0, ts*1000000).UTC()
				}
				return rows.Err()
			})
	})
	return
}

func (self *Database) GetDeviceFirstTime() (firstTime map[string]time.Time, err error) {
	firstTime = make(map[string]time.Time)

	self.runReadQuery(func(query_id string, db *Database) {
		err = db.prepareAndRunQueryWithCallback(`
		SELECT
			n,t
		FROM (
			SELECT
				deviceid AS n,
				sensors.timestamp AS t
			FROM sensors
			ORDER BY timestamp desc) GROUP BY n`,
			func(rows *sql.Rows) error {
				for rows.Next() {
					var name string
					var ts int64
					err = rows.Scan(&name, &ts)
					if err != nil {
						return errors.Wrap(err, "error while scanning row")
					}

					firstTime[name] = time.Unix(0, ts*1000000).UTC()
				}
				return rows.Err()
			})
	})
	return
}

func (self *Database) GetDeviceCountsFromDevices(devices []string) (counts map[string]int, err error) {

	counts = make(map[string]int)

	if 0 == len(devices) {
		return
	}

	self.runReadQuery(func(query_id string, db *Database) {
		err = db.prepareAndRunQueryWithCallback(fmt.Sprintf(`
			SELECT
				deviceid,
				COUNT(timestamp) AS num
			FROM sensors
			WHERE deviceid IN ('%s')
			GROUP BY deviceid`, strings.Join(devices, "','")),
			func(rows *sql.Rows) error {
				for rows.Next() {
					var name string
					var count int
					err = rows.Scan(&name, &count)
					if err != nil {
						return errors.Wrap(err, "error while scanning row")
					}
					counts[name] = count
				}
				return rows.Err()
			})
	})
	return
}

func (self *Database) GetDeviceCounts() (counts map[string]int, err error) {
	counts = make(map[string]int)

	self.runReadQuery(func(query_id string, db *Database) {
		err = db.prepareAndRunQueryWithCallback(`
	SELECT
		deviceid,
		COUNT(sensors.timestamp) AS num
	FROM sensors
	GROUP BY sensors.deviceid`,
			func(rows *sql.Rows) error {
				for rows.Next() {
					var name string
					var count int
					err = rows.Scan(&name, &count)
					if err != nil {
						return errors.Wrap(err, "error while scanning row")
					}
					counts[name] = count
				}
				return rows.Err()
			})
	})
	return
}

func (self *Database) GetLocationCounts() (counts map[string]int, err error) {
	counts = make(map[string]int)
	self.runReadQuery(func(query_id string, db *Database) {
		err = db.prepareAndRunQueryWithCallback(`
		SELECT
			sensors.locationid,
			COUNT(sensors.timestamp) AS num
		FROM sensors
		GROUP BY
			sensors.locationid`,
			func(rows *sql.Rows) error {
				for rows.Next() {
					var name string
					var count int
					err = rows.Scan(&name, &count)
					if err != nil {
						return errors.Wrap(err, "error while scanning row")
					}
					counts[name] = count
				}
				return rows.Err()
			})
	})
	return
}

// GetAllForClassification will return a sensor data for classifying
func (self *Database) GetAllForClassification(clbk func(s []models.SensorData, err error)) {
	self.runReadQuery(func(query_id string, db *Database) {
		s, err := self.GetAllFromQuery("SELECT " + SENSOR_SQL + " FROM sensors WHERE sensors.locationid !='' ORDER BY timestamp")
		clbk(s, err)
	})
}

// GetAllForClassification will return a sensor data for classifying
func (self *Database) GetAllNotForClassification(clbk func(s []models.SensorData, err error)) {
	self.runReadQuery(func(query_id string, db *Database) {
		s, err := self.GetAllFromQuery("SELECT " + SENSOR_SQL + " FROM sensors WHERE sensors.locationid =='' ORDER BY timestamp")
		clbk(s, err)
	})
}

// GetLatest will return a sensor data for classifying
func (self *Database) GetLatest(device_id string) (s models.SensorData, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		var sensors []models.SensorData
		sensors, err = db.GetAllFromPreparedQuery("SELECT "+SENSOR_SQL+" FROM sensors WHERE deviceid=? ORDER BY timestamp DESC LIMIT 1", device_id)
		if err != nil {
			return
		}
		if len(sensors) > 0 {
			s = sensors[0]
		} else {
			err = errors.New("no rows found")
		}
	})
	return
}

func (self *Database) GetKeys(keylike string) (keys []string, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		stmt, err := db.PrepareQuery(`SELECT key FROM keystore WHERE key LIKE ?`)
		if nil != err {
			return
		}
		defer stmt.Close()

		rows, err := stmt.Query(keylike)
		if err != nil {
			err = errors.Wrap(err, "error running query")
			return
		}
		defer rows.Close()

		keys = []string{}
		for rows.Next() {
			var key string
			err = rows.Scan(&key)
			if err != nil {
				err = errors.Wrap(err, "scanning")
				return
			}
			keys = append(keys, key)
		}
		err = rows.Err()
		if err != nil {
			err = errors.Wrap(err, "rows")
		}
	})
	return
}

func (self *Database) GetDevices() (devices []string, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		err = db.prepareAndRunQueryWithCallback(`
		SELECT
			devicename
		FROM (
			SELECT
				deviceid AS devicename,
				COUNT(deviceid) AS counts
			FROM sensors
			GROUP BY deviceid)
			ORDER BY counts DESC`,
			func(rows *sql.Rows) error {
				devices = []string{}
				for rows.Next() {
					var name string
					err = rows.Scan(&name)
					if err != nil {
						return errors.Wrap(err, "error while scanning row")
					}
					devices = append(devices, name)
				}
				return rows.Err()
			})
	})
	return
}

func (self *Database) GetLocations() (locations []string, err error) {
	self.runReadQuery(func(query_id string, db *Database) {
		err = db.prepareAndRunQueryWithCallback(`SELECT DISTINCT locationid FROM location_predictions`,
			func(rows *sql.Rows) error {
				locations = []string{}
				for rows.Next() {
					var name string
					err = rows.Scan(&name)
					if err != nil {
						return errors.Wrap(err, "error while scanning row")
					}
					locations = append(locations, name)
				}
				return rows.Err()
			})
	})
	return
}

func GetFamilies() (families []string) {
	files, err := ioutil.ReadDir(DataFolder)
	if err != nil {
		log.Fatal(err)
	}

	families = make([]string, len(files))
	i := 0
	for _, f := range files {
		if !strings.Contains(f.Name(), ".sqlite3.db") {
			continue
		}
		b, err := base58.Decode(strings.TrimSuffix(f.Name(), ".sqlite3.db"))
		if err != nil {
			continue
		}
		families[i] = string(b)
		i++
	}
	if i > 0 {
		families = families[:i]
	} else {
		families = []string{}
	}
	return
}

func (self *Database) DeleteLocation(location_id string) (err error) {
	self.runQueryAsync(func(query_id string) {
		stmt, err := self.PrepareQuery("DELETE FROM sensors WHERE locationid = ?")
		if nil != err {
			Fatal(err, "DeleteLocation Prepare")
		}
		defer stmt.Close()

		_, err = stmt.Exec(location_id)
		if nil != err {
			Fatal(err, "DeleteLocation Execute")
		}
	})
	return
}

func Exists(name string) (err error) {
	name = strings.TrimSpace(name)
	name = path.Join(DataFolder, base58.FastBase58Encoding([]byte(name))+".sqlite3.db")
	if _, err = os.Stat(name); err != nil {
		err = errors.New("database '" + name + "' does not exist")
	}
	return
}

func (self *Database) Delete() (err error) {
	// logger.Log.Debugf("deleting %s", self.family)
	return os.Remove(self.name)
}

// Open will open the database for transactions by first aquiring a filelock.
func Open(family string, readOnly ...bool) (d *Database, err error) {
	d = new(Database)
	d.family = strings.TrimSpace(family)

	// convert the name to base64 for file writing
	// override the name
	if len(readOnly) > 1 && readOnly[1] {
		d.name = path.Join(DataFolder, d.family)
	} else {
		d.name = path.Join(DataFolder, base58.FastBase58Encoding([]byte(d.family))+".sqlite3.db")
	}

	// if read-only, make sure the database exists
	if _, err = os.Stat(d.name); err != nil && len(readOnly) > 0 && readOnly[0] {
		err = errors.New(fmt.Sprintf("group '%s' does not exist", d.family))
		return
	}

	// // check if it is a new database
	newDatabase := false
	if _, err := os.Stat(d.name); os.IsNotExist(err) {
		newDatabase = true
	}

	// open sqlite3 database
	d.db, err = sql.Open("sqlite3", d.name+"?cache=shared&mode=rwc&_busy_timeout=50000000")
	if err != nil {
		return
	}

	// create new database tables if needed
	if newDatabase {
		err = d.MakeTables()
		if err != nil {
			return
		}
	}
	d.StartRequestQueue()

	return
}

func (self *Database) Debug(debugMode bool) {
	if debugMode {
		logger.SetLevel("debug")
	} else {
		logger.SetLevel("info")
	}
}

// Close will close the database connection and remove the filelock.
func (self *Database) Close() (err error) {
	if self.isClosed {
		return
	}
	// close database
	err2 := self.db.Close()
	if err2 != nil {
		err = err2
		logger.Log.Error(err)
	}
	self.isClosed = true
	return
}

func (self *Database) GetAllFromQuery(query string) (s []models.SensorData, err error) {
	logger.Log.Trace(query)

	rows, err := self.db.Query(query)
	if err != nil {
		err = errors.Wrap(err, "GetAllFromQuery")
		return
	}
	defer rows.Close()

	// parse rows
	s, err = self.getRows(rows)
	if err != nil {
		err = errors.Wrap(err, query)
	}

	return
}

// GetAllFromPreparedQuery
func (self *Database) GetAllFromPreparedQuery(query string, args ...interface{}) (s []models.SensorData, err error) {
	stmt, err := self.PrepareQuery(query)
	if nil != err {
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()
	s, err = self.getRows(rows)
	if err != nil {
		err = errors.Wrap(err, query)
	}
	return
}

func (self *Database) getRows(rows *sql.Rows) (s []models.SensorData, err error) {

	s = []models.SensorData{}
	// loop through rows
	for rows.Next() {
		var row string
		var sensor models.SensorRow
		err = rows.Scan(&row)
		if err != nil {
			panic(err)
			err = errors.Wrap(err, "getRows")
			return
		}

		err = json.Unmarshal([]byte(row), &sensor)
		if err != nil {
			fmt.Println(row)
			panic(err)
			err = errors.Wrap(err, "getRows")
			return
		}

		s0 := models.SensorData{
			// the underlying value of the interface pointer and cast it to a pointer interface to cast to a byte to cast to a string
			Timestamp: sensor.Timestamp,
			Family:    self.family,
			Device:    sensor.DeviceId,
			Location:  sensor.LocationId,
			Sensors:   make(map[string]map[string]interface{}),
		}

		s0.Sensors[sensor.SensorType] = sensor.Sensor

		s = append(s, s0)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
		err = errors.Wrap(err, "getRows")
	}
	return
}

// SetGPS will set a GPS value in the GPS database
func (self *Database) SetGPS(p models.SensorData) (err error) {
	self.runQueryAsync(func(query_id string) {
		tx, err := self.db.Begin()
		if err != nil {
			Fatal(err, "SetGPS Begin")
		}
		stmt, err := tx.Prepare("INSERT OR REPLACE INTO gps(mac, loc, lat, lon, alt) VALUES (?, ?, ?, ?, ?)")
		if err != nil {
			Fatal(err, "SetGPS Prepare")
		}
		defer stmt.Close()

		for sensorType := range p.Sensors {
			for mac := range p.Sensors[sensorType] {
				_, err = stmt.Exec(sensorType+"-"+mac, p.Location, p.GPS.Latitude, p.GPS.Longitude, p.GPS.Altitude)
				if err != nil {
					Fatal(err, "SetGPS Execute")
				}
			}
		}

		err = tx.Commit()
		if err != nil {
			Fatal(err, "SetGPS Commit")
		}
	})
	return
}

func (self *Database) StartRequestQueue() {
	self.requestQueue = make(chan func(query_id string), 100)
	var c int64 = 0
	go func() {
		for request_func := range self.requestQueue {
			c++

			t1 := time.Now()
			query_id := self.getQId("w")
			logger.Log.Tracef("Running query %v", query_id)
			request_func(query_id)
			logger.Log.Tracef("Finished query %v %v", query_id, time.Since(t1))
		}
	}()
}

// Generate query id for debugging
func (self *Database) getQId(mode string) string {
	self.lock.Lock()
	self.num_queries++
	query_id := fmt.Sprintf("%v%v%6v", mode, GetMD5Hash(self.family)[0:4], strconv.FormatInt(self.num_queries, 16))
	query_id = strings.Replace(query_id, " ", "x", -1)
	self.lock.Unlock()
	return query_id
}

func (self *Database) GetPending() int {
	return len(self.requestQueue)
}

func Fatal(err error, wrapper string) {
	err = errors.Wrap(err, wrapper)
	panic(err)
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

// // GetGPS will return a GPS for a given mac, if it exists
// // if it doesn't exist it will return an error
// func (d *Database) GetGPS(mac string) (gps models.GPS, err error) {
// 	query := "SELECT mac,lat,lon,alt,timestamp FROM gps WHERE mac == ?"
// 	stmt, err := d.db.Prepare(query)
// 	if err != nil {
// 		err = errors.Wrap(err, query)
// 		return
// 	}
// 	defer stmt.Close()
// 	rows, err := stmt.Query(mac)
// 	if err != nil {
// 		err = errors.Wrap(err, query)
// 		return
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		err = rows.Scan(&gps.Mac, &gps.Latitude, &gps.Longitude, &gps.Altitude, &gps.Timestamp)
// 		if err != nil {
// 			err = errors.Wrap(err, "scanning")
// 			return
// 		}
// 	}
// 	err = rows.Err()
// 	if err != nil {
// 		err = errors.Wrap(err, "rows")
// 	}
// 	if gps.Mac == "" {
// 		err = errors.New(mac + " does not exist in gps table")
// 	}
// 	return
// }
