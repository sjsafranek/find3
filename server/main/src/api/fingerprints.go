package api

import (
	"sync"
	"time"

	"github.com/schollz/find4/server/main/src/database"
	"github.com/schollz/find4/server/main/src/models"
)

type UpdateCounterMap struct {
	// Data maps family -> counts of locations
	Count map[string]int
	sync.RWMutex
}

var globalUpdateCounter UpdateCounterMap

func init() {
	globalUpdateCounter.Lock()
	defer globalUpdateCounter.Unlock()
	globalUpdateCounter.Count = make(map[string]int)
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
		go updateCounter(db, p.Family)
	}
	return
}

// SavePrediction will add sensor data to the database
func SavePrediction(db *database.Database, s models.SensorData, p models.LocationAnalysis) (err error) {
	err = db.AddPrediction(s.Timestamp, p.Guesses)
	return
}

func updateCounter(db *database.Database, family string) {
	globalUpdateCounter.Lock()
	if _, ok := globalUpdateCounter.Count[family]; !ok {
		globalUpdateCounter.Count[family] = 0
	}
	globalUpdateCounter.Count[family]++
	count := globalUpdateCounter.Count[family]
	globalUpdateCounter.Unlock()

	logger.Log.Debugf("'%s' has %d new fingerprints", family, count)
	if count < 5 {
		return
	}

	var lastCalibrationTime time.Time
	err := db.Get("LastCalibrationTime", &lastCalibrationTime)
	if err == nil {
		if time.Since(lastCalibrationTime) < 5*time.Minute {
			return
		}
	}
	logger.Log.Infof("have %d new fingerprints for '%s', re-calibrating since last calibration was %s", count, family, time.Since(lastCalibrationTime))
	globalUpdateCounter.Lock()
	globalUpdateCounter.Count[family] = 0
	globalUpdateCounter.Unlock()

	// debounce the calibration time
	err = db.Set("LastCalibrationTime", time.Now().UTC())
	if err != nil {
		logger.Log.Error(err)
	}

	// if any errors occur they get swallowed
	go Calibrate(db, family, true)
}
