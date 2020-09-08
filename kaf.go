package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
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
