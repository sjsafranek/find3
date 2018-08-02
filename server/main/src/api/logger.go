package api

// import "github.com/schollz/find3/server/main/src/logging"
import "github.com/sjsafranek/ligneous"

var logger ligneous.SeelogWrapper

func init() {
	logger = ligneous.New()
	Debug(false)
}

func Debug(debugMode bool) {
	if debugMode {
		logger.SetLevel("trace")
	} else {
		logger.SetLevel("info")
	}
}
