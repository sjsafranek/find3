package database

import (
	"database/sql"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sjsafranek/ligneous"
)

// DataFolder is set to where you want each Sqlite3 database to be stored
var DataFolder = "."

// Database is the main structure for holding the information
// pertaining to the name of the database.
type Database struct {
	name         string
	family       string
	db           *sql.DB
	logger       *ligneous.SeelogWrapper
	isClosed     bool
	requestQueue chan func(string)
	num_queries  int64
	lock         sync.RWMutex
}

// type DatabaseLock struct {
// 	Locked map[string]bool
// 	sync.RWMutex
// }
//
// var databaseLock *DatabaseLock
//
// func init() {
// 	databaseLock = new(DatabaseLock)
// 	databaseLock.Lock()
// 	defer databaseLock.Unlock()
// 	databaseLock.Locked = make(map[string]bool)
// }
