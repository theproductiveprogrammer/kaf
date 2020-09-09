package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

/*    understand/
 * main entry point into our program
 * (It all starts here)
 *
 *    way/
 * Get the user configuration, load existing log files, and start the
 * server
 */
func main() {
	cfg := getConfig()
	if cfg == nil {
		showHelp()
		return
	}

	logsChan := loadExistingLogs(cfg.dbloc)
	startServer(cfg, logsChan)

/*
 * important types. Helper types at end of file
 */

/*    understand/
 * event logs are managed by a goroutine represented by this struct.
 * Because it is a goroutine, we communicate with it via a channel -
 * making requests for message logs
 */
type logsRoutine struct {
	c chan logReq
}

/*    understand/
 * represents a request for a message log from the main logsRoutine
 * goroutine. We can request for the log to be created if it doesn't
 * exist and we expect our response to be sent back via the channel we
 * provide (either the message log itself or an error)
 */
type logReq struct {
	name     string
	noCreate bool

	resp chan logReqResp
}
type logReqResp struct {
	msglog *msgLog
	err    error
}

/*    understand/
 * similar to logsRoutine, each message log is also handled by it's own
 * goroutine. We communicate to it via it's channels - either to get
 * message logs or to put a new message log.
 */
type msgLog struct {
	name string
	get  chan getReq
	put  chan putReq
}

/*    understand/
 * represents a request to a message log to get messages and hands over
 * a channel where we expect the responses
 */
type getReq struct {
	num  int
	resp chan getReqResp
}
type getReqResp struct {
	msg []msg
	err error
}

/*    understand/
 * represents a request to a message log to put messages and hands over
 * a channel where we expect the response or error
 */
type putReq struct {
	data io.Reader
	resp chan putReqResp
}
type putReqResp struct {
	num uint32
	err error
}

/*    understand/
 * represents a message in the event log
 */
type msg struct {
	num  uint32
	sz   uint32
	data []byte
	err  error
}

/*    way/
 * Load configuration from the command line
 */
func getConfig() *config {
	if len(os.Args) != 3 {
		return nil
	}
	return &config{
		addr:  os.Args[1],
		dbloc: os.Args[2],
	}
}

func showHelp() {
	fmt.Println("kaf: Simple Event Store")
	fmt.Println("eg: go run kaf 127.0.0.1:7749 ../kafdata")
	fmt.Println("    go run kaf <addr> <path to data folder>")
}

func loadExistingLogs(dbloc string) chan logReq {
	return nil
}

func startServer(cfg *config, logsChan chan logReq) {
	setupRequestHandlers(cfg, logsChan)

	log.Println("Starting server on", cfg.addr, "writing to", cfg.dbloc)
	log.Fatal(http.ListenAndServe(cfg.addr, nil))
}

func setupRequestHandlers(cfg *config, logsChan chan logReq) {
	wrapH := func(h reqHandler) httpHandler {
		return func(w http.ResponseWriter, r *http.Request) {
			h(cfg, r, logsChan, w)
		}
	}
	http.HandleFunc("/put/", wrapH(put))
}

func put(cfg *config, r *http.Request, logsChan chan logReq, w http.ResponseWriter) {
	name := strings.TrimSpace(r.URL.Path[len("/put/"):])
	if len(name) == 0 {
		err_("Missing event log name", 400, r, w)
		return
	}
}

func err_(error string, code int, r *http.Request, w http.ResponseWriter) {
	log.Println("ERROR:", r.RemoteAddr, r.RequestURI, error)
	http.Error(w, error, code)
}

type config struct {
	addr  string
	dbloc string
}

type logReq struct {
	name string
}

type reqHandler func(*config, *http.Request, chan logReq, http.ResponseWriter)
type httpHandler func(http.ResponseWriter, *http.Request)
