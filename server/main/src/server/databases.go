package server

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	// "github.com/schollz/find4/server/main/src/api"
	"github.com/schollz/find4/server/main/src/database"
)

var (
	DATABASES map[string]*database.Database
)

func OpenDatabase(family string) error {
	db_conn, err := database.Open(family, false)
	if nil != err {
		return err
	}
	DATABASES[family] = db_conn
	// testing
	// logger.Log.Info("Calibrating on database startup.")
	// api.Calibrate(db_conn, family, true)
	return nil
}

func GetDatabase(family string) (*database.Database, error) {
	// return database.Open(family, false)
	if _, ok := DATABASES[family]; !ok {
		err := OpenDatabase(family)
		return DATABASES[family], err
	}
	return DATABASES[family], nil
}

func DeleteDatabase(family string) error {
	db, err := GetDatabase(family)
	if nil != err {
		return err
	}
	db.Delete()
	DATABASES[family].Close()
	delete(DATABASES, family)
	return nil
}

func init() {
	DATABASES = make(map[string]*database.Database)

	go func() {
		for {
			time.Sleep(10 * time.Second)

			c := 0
			for range DATABASES {
				c++
			}
			logger.Log.Debugf("%v databases", c)

			for family := range DATABASES {
				logger.Log.Debugf("%v requests in %v queue", DATABASES[family].GetPending(), family)
			}
		}
	}()

	signal_queue := make(chan os.Signal)
	signal.Notify(signal_queue, syscall.SIGTERM)
	signal.Notify(signal_queue, syscall.SIGINT)
	go func() {
		sig := <-signal_queue
		logger.Log.Warnf("caught sig: %+v", sig)
		logger.Log.Warn("Gracefully shutting down...")
		for family := range DATABASES {
			logger.Log.Warnf("Closing %v database", family)
			DATABASES[family].Close()
		}
		logger.Log.Warn("Shutting down...")
		os.Exit(0)
	}()

}
