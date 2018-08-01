package api

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/sjsafranek/pool"
)

var (
	AI_SERVER_ADDRESS string = "localhost:7005"
	aiPool            pool.Pool
	AI_PENDING        int = 0
)

func init() {
	factory := func() (net.Conn, error) { return net.Dial("tcp", AI_SERVER_ADDRESS) }
	pool, err := pool.NewChannelPool(4, 8, factory)
	if nil != err {
		panic(err)
	}
	aiPool = pool
}

const RETRY_LIMIT int = 2

func aiSendAndRecieveWithRetry(query string, attempt int) (string, error) {

	if RETRY_LIMIT < attempt {
		err := errors.New("retry limit reached")
		logger.Log.Error(err)
		logger.Log.Error(query)
		return "", err
	}

	conn, err := aiPool.Get()
	logger.Log.Debug("got socket connection")
	if nil != err {
		panic(err)
	}
	defer conn.Close()

	payload := fmt.Sprintf("%v\r\n", query)
	fmt.Fprintf(conn, payload)

	results, err := bufio.NewReader(conn).ReadString('\n')
	if nil != err {
		logger.Log.Error(err)
		attempt++
		logger.Log.Warnf("unable to read from socket, attempt=%v", attempt)
		logger.Log.Warn("removing socket from pool")
		pc := conn.(*pool.PoolConn)
		pc.MarkUnusable()
		pc.Close()

		return aiSendAndRecieveWithRetry(query, attempt)
	}

	if pc, ok := conn.(*pool.PoolConn); !ok {
		logger.Log.Warn("socket is unusable, removing from pool")
		pc.MarkUnusable()
		pc.Close()
	}

	return results, nil
}

func aiSendAndRecieve(query string) (string, error) {
	logger.Log.Tracef("IN  %v", query)
	logger.Log.Debug("sending message to ai server")
	results, err := aiSendAndRecieveWithRetry(query, 1)
	// conn, err := aiPool.Get()
	// logger.Log.Info("aiSendAndRecieve [CONNECT]")
	// if nil != err {
	// 	panic(err)
	// }
	// defer conn.Close()
	//
	// payload := fmt.Sprintf("%v\r\n", query)
	// fmt.Fprintf(conn, payload)
	//
	// results, err := bufio.NewReader(conn).ReadString('\n')
	// if nil != err {
	// 	logger.Log.Error(err)
	// 	logger.Log.Warn("aiSendAndRecieve [RETRY]")
	// 	return aiSendAndRecieve(query)
	// }
	//
	// if pc, ok := conn.(*pool.PoolConn); !ok {
	// 	logger.Log.Warn("socket is unusable, removing from pool")
	// 	pc.MarkUnusable()
	// 	pc.Close()
	// }
	logger.Log.Tracef("OUT %v", results)
	return results, err
}
