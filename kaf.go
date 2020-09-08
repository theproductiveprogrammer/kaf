package main

import (
	"fmt"
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

func getConfig() *config {
	return nil
}

func showHelp() {
	fmt.Println("kaf: Simple Event Store")
	fmt.Println("go run kaf <addr> <path to data folder>")
}

func loadExistingLogs(dbloc string) chan logReq {
	return nil
}

func startServer(cfg *config, logsChan chan logReq) {
}

type config struct {
	addr  string
	dbloc string
}

type logReq struct {
	name string
}
