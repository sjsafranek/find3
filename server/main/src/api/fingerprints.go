package api

import (
	"sync"
	// "sync/atomic"
	"time"

	"github.com/schollz/find4/server/main/src/database"
	"github.com/schollz/find4/server/main/src/models"
)

type UpdateCounterMap struct {
	Queues map[string]time.Time
	sync.RWMutex
}

var globalUpdateCounter UpdateCounterMap

func init() {
	globalUpdateCounter.Lock()
	globalUpdateCounter.Queues = make(map[string]time.Time)
	globalUpdateCounter.Unlock()
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

	if p.Location != "" {
		// database triggers this
		go TriggerClassifyEvent(db, p.Family)
	}
	return
}

// SavePrediction will add sensor data to the database
func SavePrediction(db *database.Database, s models.SensorData, p models.LocationAnalysis) (err error) {
	err = db.AddPrediction(s.Timestamp, p.Guesses)
	return
}

func TriggerClassifyEvent(db *database.Database, family string) {
	globalUpdateCounter.Lock()
	if _, ok := globalUpdateCounter.Queues[family]; !ok {
		go calibrationWorker(db, family)
	}
	globalUpdateCounter.Queues[family] = time.Now()
	globalUpdateCounter.Unlock()
}

func calibrationWorker(db *database.Database, family string) {
	last_classification_time := time.Now()
	last_classification_event_time := time.Now()
	for {

		logger.Log.Critical(db.LastInsertTime.Sub(last_classification_time))
		logger.Log.Critical(1*time.Minute < db.LastInsertTime.Sub(last_classification_time))
		logger.Log.Critical(last_classification_event_time != globalUpdateCounter.Queues[family])
		if 1*time.Minute < db.LastInsertTime.Sub(last_classification_time) || last_classification_event_time != globalUpdateCounter.Queues[family] {

			// if last_classification_time != globalUpdateCounter.Queues[family] {
			last_classification_event_time = globalUpdateCounter.Queues[family]

			logger.Log.Warnf("Calibrating %v...", family)

			// if any errors occur they get swallowed
			err := Calibrate(db, family, true)
			if nil != err {
				logger.Log.Error(err)
				continue
			}

			// debounce the calibration time
			err = db.Set("LastCalibrationTime", time.Now().UTC())
			if err != nil {
				logger.Log.Error(err)
			}
			logger.Log.Infof("Calibration for %v complete", family)

			last_classification_time = time.Now()
		}

		// }

		time.Sleep(60 * time.Second)
	}

}
