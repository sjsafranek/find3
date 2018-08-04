package api

import (
	"time"

	"github.com/schollz/find4/server/main/src/database"
	"github.com/schollz/find4/server/main/src/models"
)

var (
	calibration_queue chan func()
)

func init() {
	// make queue length of 1 to block channel
	calibration_queue = make(chan func(), 2)
	go calibrationWorker()
	go calibrationWorker()
}

// SaveSensorData will add sensor data to the database
func SaveSensorData(db *database.Database, p models.SensorData) (err error) {
	err = p.Validate()
	if err != nil {
		return
	}

	err = db.AddSensor(p)
	if p.GPS.Longitude != 0 && p.GPS.Latitude != 0 {
		db.SetGPS(p)
	}

	if err != nil {
		return
	}

	// if p.Location != "" {
	// database triggers this
	// go TriggerClassifyEvent(db, p.Family)
	// }
	return
}

// SavePrediction will add sensor data to the database
func SavePrediction(db *database.Database, s models.SensorData, p models.LocationAnalysis) (err error) {
	err = db.AddPrediction(s.Timestamp, p.Guesses)
	return
}

func DatabaseWorker(db *database.Database, family string) {
	var last_sensor_insert_timestamp int64
	for {

		should_calibrate := false

		// check last calibration time
		var last_calibration_time time.Time
		err := db.Get("LastCalibrationTime", &last_calibration_time)
		if nil != err {
			logger.Log.Error(err)
			should_calibrate = true
		}

		// check last sensor timestamp
		ts, err := db.GetLastSensorTimestampWithLocationId()
		if nil != err {
			logger.Log.Error(err)
			should_calibrate = true
		}

		// check against last value (historic datasets)
		if ts != last_sensor_insert_timestamp {
			last_sensor_insert_timestamp = ts
			last_sensor_insert := time.Unix(ts/1000, 0)

			if 2*time.Minute < last_sensor_insert.Sub(last_calibration_time) {
				should_calibrate = true
			}

		}

		// calibrate database or pass
		if should_calibrate {
			calibration_queue <- func() {
				logger.Log.Warnf("Calibrating %v...", family)
				// if any errors occur they get swallowed
				err := Calibrate(db, family, true)
				if nil != err {
					logger.Log.Error(err)
					return
				}
				logger.Log.Infof("Calibration for %v complete", family)
			}
		} else {
			logger.Log.Warnf("Calibration not needed for %v", family)
		}

		time.Sleep(60 * time.Second)
	}
}

func calibrationWorker() {
	for clbk := range calibration_queue {
		clbk()
	}
}
