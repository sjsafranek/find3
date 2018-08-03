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
	// "github.com/schollz/find4/server/main/src/api"
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
	logger.Log.Debugf("create database tables for %v", self.family)
	_, err = self.db.Exec(TABLES_SQL)
	if err != nil {
		logger.Log.Error(err)
		Fatal(err, "failed to create database tables")
		return
	}
	return
}

func (self *Database) queryRow(query string, scanner func(*sql.Row) error, args ...interface{}) error {
	logger.Log.Trace(query)

	stmt, err := self.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	row := stmt.QueryRow(args...)

	return scanner(row)
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

func (self *Database) prepareAndRunQueryWithCallback(query string, clbk func(*sql.Rows) error) error {
	stmt, err := self.PrepareQuery(query)
	if nil != err {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if nil != err {
		return err
	}
	defer rows.Close()

	return clbk(rows)
}

func (self *Database) runQuery(query string, eachRow func(*sql.Rows) error) error {
	stmt, err := self.PrepareQuery(query)
	if nil != err {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if nil != err {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err := eachRow(rows)
		if nil != err {
			return err
		}
	}
	return rows.Err()

}

func (self *Database) insertSync(clbk func(string)) {
	var wg sync.WaitGroup
	wg.Add(1)
	self.requestQueue <- func(query_id string) {
		defer wg.Done()
		clbk(query_id)
	}
	wg.Wait()
}

func (self *Database) insertAsync(clbk func(string)) {
	self.requestQueue <- func(query_id string) {
		clbk(query_id)
	}
}

func (self *Database) Select(clbk func(string, *Database) error) error {
	query_id := self.getQId("r")
	// open database for reading
	reader, err := Open(self.family)
	if nil != err {
		Fatal(err, "Could not open database")
		return err
	}
	defer reader.Close()

	// run callback
	logger.Log.Tracef("Running SELECT query %v", query_id)
	t1 := time.Now()
	err = clbk(query_id, reader)
	logger.Log.Tracef("Finished SELECT query %v %v", query_id, time.Since(t1))
	return err
}

// Get will retrieve the value associated with a key.
// channel event listener makes database call synchronous
func (self *Database) Get(key string, v interface{}) error {
	return self.Select(func(query_id string, db *Database) error {
		var result string
		err := self.queryRow("SELECT value FROM keystore WHERE key = ?", func(row *sql.Row) error {
			return row.Scan(&result)
		}, key)

		if nil != err {
			return err
		}

		return json.Unmarshal([]byte(result), &v)
	})
}

func (self *Database) Insert(query string, executor func(*sql.Stmt) error) error {
	query_id := self.getQId("w")
	return self.insert(query_id, query, executor)
}

func (self *Database) insert(query_id string, query string, executor func(*sql.Stmt) error) error {
	logger.Log.Tracef("%v %v", query_id, query)

	tx, err := self.db.Begin()
	if nil != err {
		return err
	}

	stmt, err := tx.Prepare(query)
	if nil != err {
		return err
	}
	defer stmt.Close()

	err = executor(stmt)
	if nil != err {
		return err
	}

	err_commit := tx.Commit()
	if nil != err {
		err_rollback := tx.Rollback()
		if nil != err_rollback {
			Fatal(err_rollback, "Unable to rollback")
		}
		return err_commit
	}

	return nil
}

// Set will set a value in the database, when using it like a keystore.
func (self *Database) Set(key string, value interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	self.insertAsync(func(query_id string) {
		self.insert(query_id, "INSERT OR REPLACE INTO keystore(key,value) VALUES (?, ?)", func(stmt *sql.Stmt) error {
			_, err = stmt.Exec(key, string(b))
			return err
		})
	})
	return nil
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
func (self *Database) AddPrediction(timestamp int64, aidata []models.LocationPrediction) error {
	// make sure we have a prediction
	if len(aidata) == 0 {
		return errors.New("no predictions to add")
	}

	self.insertAsync(func(query_id string) {
		self.insert(query_id, "INSERT OR REPLACE INTO location_predictions (timestamp, locationid, probability) VALUES (?, ?, ?)", func(stmt *sql.Stmt) error {
			for i := range aidata {
				aidata[i].Probability = float64(int64(float64(aidata[i].Probability)*100)) / 100
				_, err := stmt.Exec(timestamp, aidata[i].Location, aidata[i].Probability)
				if err != nil {
					return err
				}
			}
			return nil
		})
	})

	return nil
}

// GetPrediction will retrieve models.LocationAnalysis associated with that timestamp
func (self *Database) GetPrediction(timestamp int64) ([]models.LocationPrediction, error) {
	var aidata []models.LocationPrediction
	var result string

	err := self.Select(func(query_id string, db *Database) error {
		return self.queryRow(`
		SELECT '[' ||
			(SELECT IFNULL(GROUP_CONCAT(prediction), '') FROM (
				SELECT `+LOCATION_PREDICTION_SQL+` AS prediction
			 	FROM location_predictions WHERE timestamp = ?
			))
		|| ']'`, func(row *sql.Row) error {
			return row.Scan(&result)
		}, timestamp)
	})

	if nil != err {
		return aidata, err
	}

	// unmarshal outside of select to close database faster
	err = json.Unmarshal([]byte(result), &aidata)
	return aidata, err
}

// AddSensor will insert a sensor data into the database
// TODO: AddSensor should be special case of AddSensors
func (self *Database) AddSensor(s models.SensorData) (err error) {

	device_id := s.Device
	location_id := ""
	if len(s.Location) > 0 {
		location_id = s.Location
	}

	self.insertAsync(func(query_id string) {
		self.insert(query_id, "INSERT OR REPLACE INTO sensors(timestamp, deviceid, locationid, sensor_type, sensor) VALUES (?, ?, ?, ?, ?)", func(stmt *sql.Stmt) error {
			for sensor_type, sensor := range s.Sensors {
				data, _ := json.Marshal(sensor)
				_, err = stmt.Exec(s.Timestamp, device_id, location_id, sensor_type, string(data))
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
	return
}

// GetSensorFromTime will return a sensor data for a given timestamp
func (self *Database) GetSensorFromTime(timestamp interface{}) (models.SensorData, error) {
	var s models.SensorData
	err := self.Select(func(query_id string, db *Database) error {
		sensors, err := db.GetAllFromQuery("SELECT "+SENSOR_SQL+" FROM sensors WHERE timestamp = ?", timestamp)
		if err != nil {
			return err
		}
		s = sensors[0]
		return nil
	})
	return s, err
}

// Get will retrieve the value associated with a key.
func (self *Database) GetLastSensorTimestamp() (int64, error) {
	var timestamp int64
	err := self.Select(func(query_id string, db *Database) error {
		return self.queryRow("SELECT timestamp FROM sensors ORDER BY timestamp DESC LIMIT 1", func(row *sql.Row) error {
			return row.Scan(&timestamp)
		})
	})
	return timestamp, err
}

// Get will retrieve the value associated with a key.
func (self *Database) TotalLearnedCount() (int64, error) {
	var count int64
	err := self.Select(func(query_id string, db *Database) error {
		return self.queryRow("SELECT count(timestamp) FROM sensors WHERE locationid != ''", func(row *sql.Row) error {
			return row.Scan(&count)
		})
	})
	return count, err
}

// TODO
//  - single transaction
// GetSensorFromGreaterTime will return a sensor data for a given timeframe
func (self *Database) GetSensorFromGreaterTime(timeBlockInMilliseconds int64) ([]models.SensorData, error) {
	var sensors []models.SensorData
	err := self.Select(func(query_id string, db *Database) error {
		latestTime, err := db.GetLastSensorTimestamp()
		if err != nil {
			return err
		}
		minimumTimestamp := latestTime - timeBlockInMilliseconds
		sensors, err = db.GetAllFromQuery("SELECT "+SENSOR_SQL+" FROM (SELECT * FROM sensors WHERE timestamp > ? GROUP BY deviceid ORDER BY timestamp DESC)", minimumTimestamp)
		return nil
	})
	return sensors, err
}

func (self *Database) NumDevices() (int, error) {
	var num int
	err := self.Select(func(query_id string, db *Database) error {
		return self.queryRow("SELECT COUNT(DISTINCT deviceid) FROM sensors WHERE deviceid != ''", func(row *sql.Row) error {
			return row.Scan(&num)
		})
	})
	return num, err
}

func (self *Database) GetDeviceFirstTimeFromDevices(devices []string) (map[string]time.Time, error) {
	firstTime := make(map[string]time.Time)

	if 0 == len(devices) {
		return firstTime, nil
	}

	err := self.Select(func(query_id string, db *Database) error {
		return db.runQuery(fmt.Sprintf(`
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
				var name string
				var ts int64
				err := rows.Scan(&name, &ts)
				if err != nil {
					return errors.Wrap(err, "error while scanning row")
				}
				firstTime[name] = time.Unix(0, ts*1000000).UTC()
				return nil
			})
	})
	return firstTime, err
}

func (self *Database) GetDeviceFirstTime() (map[string]time.Time, error) {
	firstTime := make(map[string]time.Time)
	err := self.Select(func(query_id string, db *Database) error {
		return db.runQuery(`
		SELECT
			n,t
		FROM (
			SELECT
				deviceid AS n,
				sensors.timestamp AS t
			FROM sensors
			ORDER BY timestamp desc)
			GROUP BY n`,
			func(rows *sql.Rows) error {
				var name string
				var ts int64
				err := rows.Scan(&name, &ts)
				if err != nil {
					return errors.Wrap(err, "error while scanning row")
				}
				firstTime[name] = time.Unix(0, ts*1000000).UTC()
				return nil
			})
	})
	return firstTime, err
}

func (self *Database) GetDeviceCountsFromDevices(devices []string) (map[string]int, error) {
	counts := make(map[string]int)
	if 0 == len(devices) {
		return counts, nil
	}

	err := self.Select(func(query_id string, db *Database) error {
		return db.runQuery(fmt.Sprintf(`
		SELECT '{' || (
			SELECT IFNULL(GROUP_CONCAT(counts), '') FROM (
				SELECT '"' || deviceid || '": ' || COUNT(sensors.timestamp) AS counts
				FROM sensors
				WHERE deviceid IN ('%s')
				GROUP BY sensors.deviceid
			)
		) || '}'`, strings.Join(devices, "','")),
			func(rows *sql.Rows) error {
				var results string
				err := rows.Scan(&results)
				if nil != err {
					return err
				}
				return json.Unmarshal([]byte(results), &counts)

			})
	})
	return counts, err
}

func (self *Database) GetDeviceCounts(devices []string) (map[string]int, error) {
	counts := make(map[string]int)
	if 0 == len(devices) {
		return counts, nil
	}

	err := self.Select(func(query_id string, db *Database) error {
		return db.runQuery(`
		SELECT '{' || (
			SELECT IFNULL(GROUP_CONCAT(counts), '') FROM (
				SELECT '"' || deviceid || '": ' || COUNT(sensors.timestamp) AS counts
				FROM sensors
				GROUP BY sensors.deviceid
			)
		) || '}'`,
			func(rows *sql.Rows) error {
				var results string
				err := rows.Scan(&results)
				if nil != err {
					return err
				}
				return json.Unmarshal([]byte(results), &counts)
			})
	})
	return counts, err
}

func (self *Database) GetLocationCounts() (map[string]int, error) {
	counts := make(map[string]int)
	err := self.Select(func(query_id string, db *Database) error {
		return db.runQuery(`
		SELECT '{' ||
			(
				SELECT IFNULL(GROUP_CONCAT(counts), '') FROM (
					SELECT '"' || sensors.locationid || '": ' || COUNT(sensors.timestamp) AS counts
					FROM sensors
					GROUP BY sensors.locationid
				)
		) || '}'`,
			func(rows *sql.Rows) error {
				var results string
				err := rows.Scan(&results)
				if nil != err {
					return err
				}
				return json.Unmarshal([]byte(results), &counts)
			})
	})
	return counts, err
}

// GetAllForClassification will return a sensor data for classifying
func (self *Database) GetAllForClassification(clbk func(s []models.SensorData, err error)) {
	_ = self.Select(func(query_id string, db *Database) error {
		s, err := self.GetAllFromQuery("SELECT " + SENSOR_SQL + " FROM sensors WHERE sensors.locationid !='' ORDER BY timestamp")
		clbk(s, err)
		return err
	})
}

// GetAllForClassification will return a sensor data for classifying
func (self *Database) GetAllNotForClassification(clbk func(s []models.SensorData, err error)) {
	_ = self.Select(func(query_id string, db *Database) error {
		s, err := self.GetAllFromQuery("SELECT " + SENSOR_SQL + " FROM sensors WHERE sensors.locationid =='' ORDER BY timestamp")
		clbk(s, err)
		return err
	})
}

// GetLatest will return a sensor data for classifying
func (self *Database) GetLatest(device_id string) (models.SensorData, error) {
	var s models.SensorData
	err := self.Select(func(query_id string, db *Database) error {
		sensors, err := db.GetAllFromQuery("SELECT "+SENSOR_SQL+" FROM sensors WHERE deviceid=? ORDER BY timestamp DESC LIMIT 1", device_id)
		if nil != err {
			return err
		}
		if 0 == len(sensors) {
			return errors.New("no rows found")
		}
		s = sensors[0]
		return err
	})
	return s, err
}

func (self *Database) parseRowsToStringSlice(rows *sql.Rows) ([]string, error) {
	slice := []string{}
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if nil != err {
			return slice, err
		}
		slice = append(slice, name)
	}
	return slice, rows.Err()

}

func (self *Database) GetDevices() ([]string, error) {
	var devices []string
	err := self.Select(func(query_id string, db *Database) error {
		return db.runQuery(`
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
				var name string
				err := rows.Scan(&name)
				if nil != err {
					return err
				}
				devices = append(devices, name)
				return nil
			})
	})
	return devices, err
}

func (self *Database) GetLocations() ([]string, error) {
	var locations []string
	err := self.Select(func(query_id string, db *Database) error {
		return db.runQuery(`SELECT DISTINCT locationid FROM location_predictions`,
			func(rows *sql.Rows) error {
				var name string
				err := rows.Scan(&name)
				if nil != err {
					return err
				}
				locations = append(locations, name)
				return nil
			})
	})
	return locations, err
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

func (self *Database) DeleteLocation(location_id string) error {
	var err error
	self.insertAsync(func(query_id string) {
		stmt, err := self.PrepareQuery("DELETE FROM sensors WHERE locationid = ?")
		if nil != err {
			return
		}
		defer stmt.Close()

		_, err = stmt.Exec(location_id)
		if nil != err {
			return
		}
	})
	return err
}

func (self *Database) Delete() (err error) {
	// logger.Log.Debugf("deleting %s", self.family)
	return os.Remove(self.name)
}

func (self *Database) Debug(debugMode bool) {
	if debugMode {
		logger.SetLevel("trace")
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

// GetAllFromQuery
func (self *Database) GetAllFromQuery(query string, args ...interface{}) ([]models.SensorData, error) {
	// TODO
	//  - select single row and unmarshal to []models.SensorData{}

	// query := fmt.Sprintf(`
	// 	SELECT '[' ||
	// 	(
	// 		%v
	// 	)
	// 	|| ']'
	// `, query)

	logger.Log.Trace(query)

	s := []models.SensorData{}

	stmt, err := self.PrepareQuery(query)
	if nil != err {
		return s, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return s, err
	}
	defer rows.Close()

	string_slice, err := self.parseRowsToStringSlice(rows)
	if nil != err {
		return s, err
	}

	for _, item := range string_slice {
		var sensor models.SensorRow

		err = json.Unmarshal([]byte(item), &sensor)
		if err != nil {
			return s, err
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

	return s, rows.Err()
}

// SetGPS will set a GPS value in the GPS database
func (self *Database) SetGPS(p models.SensorData) error {
	self.insertAsync(func(query_id string) {
		self.insert(query_id, "INSERT OR REPLACE INTO gps(mac, loc, lat, lon, alt) VALUES (?, ?, ?, ?, ?)", func(stmt *sql.Stmt) error {
			for sensorType := range p.Sensors {
				for mac := range p.Sensors[sensorType] {
					_, err := stmt.Exec(sensorType+"-"+mac, p.Location, p.GPS.Latitude, p.GPS.Longitude, p.GPS.Altitude)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	})
	return nil
}

func (self *Database) StartRequestQueue() {
	self.requestQueue = make(chan func(query_id string), 100)
	var c int64 = 0
	go func() {
		for request_func := range self.requestQueue {
			c++

			t1 := time.Now()
			query_id := self.getQId("w")
			logger.Log.Tracef("Running INSERT query %v", query_id)
			request_func(query_id)
			logger.Log.Tracef("Finished INSERT query %v %v", query_id, time.Since(t1))

			self.LastInsertTime = time.Now()
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

func Exists(name string) (err error) {
	name = strings.TrimSpace(name)
	name = path.Join(DataFolder, base58.FastBase58Encoding([]byte(name))+".sqlite3.db")
	if _, err = os.Stat(name); err != nil {
		err = errors.New("database '" + name + "' does not exist")
	}
	return
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
