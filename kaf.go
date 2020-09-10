package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

/*    understand/
 * program version
 */
const VERSION = "0.2.0"

/*    understand/
 * main entry point into our program
 * (It all starts here)
 *
 *    way/
 * Get the user configuration, start the logs goroutine, and start the
 * server
 */
func main() {
	cfg := getConfig()
	if cfg == nil {
		showHelp()
		return
	}

	startServer(cfg, getLogsRoutine(cfg.dbloc))
}

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
	name   string
	create bool

	resp chan logReqResp
}
type logReqResp struct {
	msglog *msgLog
	err    error
}

/*    understand/
 * similar to logsRoutine, each message log is also handled by it's own
 * goroutine. We communicate to it via it's channels - either to get
 * message logs or to put a new message log or get stats.
 */
type msgLog struct {
	name  string
	get   chan getReq
	put   chan putReq
	stat  chan statReq
	stats stats
}

/*    understand/
 * represents a request to a message log to get messages and hands over
 * a channel where we expect the responses
 */
type getReq struct {
	num  uint32
	resp chan getReqResp
}
type getReqResp struct {
	msgs []*msg
	err  error
}

/*    understand/
 * represents a request to a message log to put messages and hands over
 * a channel where we expect the response or error
 */
type putReq struct {
	data []byte
	resp chan putReqResp
}
type putReqResp struct {
	num uint32
	err error
}

/*    understand/
 * represents a request to a message log get stats
 */
type statReq struct {
	resp chan stats
}

/*    understand/
 * relevant stats for a message log
 */
type stats struct {
	getCount uint32
	putCount uint32
}

/*    understand/
 * represents a message in the event log
 */
type msg struct {
	offset int64
	num    uint32
	sz     uint32
	data   []byte
}

/*
 * Data File constants
 */
const DBHeader = "KAF|v1"
const RecHeaderPfx = "\nKAF|"
const RecHeaderSfx = "\n"

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
	fmt.Println("version: " + VERSION)
}

/*    understand/
 * We use a goroutine as the single point of synchoronous
 * contact for all other goroutines to get access to
 * message logs - it creates/manages all of them
 *
 *    way/
 * load all logs from disk, set up the stat tracker,
 * then return the goroutine
 */
func getLogsRoutine(dbloc string) logsRoutine {
	c := make(chan logReq)
	logs := []*msgLog{}

	files, err := ioutil.ReadDir(dbloc)
	if err != nil {
		log.Println(err)
		log.Panic("Failed to read:", dbloc)
	}
	for _, f := range files {
		log_, err := loadLog(dbloc, f.Name())
		if err != nil {
			log.Println(err)
			log.Panic("Failed to read:", f.Name())
		}
		logs = append(logs, log_)
	}

	go func() {
		for {
			req := <-c
			msgLog := findLog(logs, req.name)
			if msgLog != nil {
				req.resp <- logReqResp{msgLog, nil}
				continue
			}

			if req.create {

				createLogFile(dbloc, req.name)

				log_, err := loadLog(dbloc, req.name)
				if err != nil {
					req.resp <- logReqResp{nil, err}
					continue
				}
				logs = append(logs, log_)
				req.resp <- logReqResp{log_, nil}

				continue
			}

			req.resp <- logReqResp{}

		}
	}()

	hasStats := func(log_ *msgLog) bool {
		return log_.stats.getCount > 0 || log_.stats.putCount > 0
	}

	ticker := time.NewTicker(5 * time.Second)
	var statCount uint32 = 0
	go func() {
		c := make(chan stats)
		var b strings.Builder

		for {
			isEmpty := true
			start := time.Now()

			<-ticker.C
			statCount++

			for _, log_ := range logs {
				log_.stat <- statReq{resp: c}
				log_.stats = <-c
				if hasStats(log_) {
					isEmpty = false
				}
			}

			end := time.Now()

			if isEmpty {
				continue
			}

			b.WriteString("{\"startTime\":\"")
			b.WriteString(start.UTC().Format(time.RFC3339))
			b.WriteString("\",\"endTime\":\"")
			b.WriteString(end.UTC().Format(time.RFC3339))
			fmt.Fprintf(&b, "\",\"stat\":%d,", statCount)
			firstDone := false
			for _, log_ := range logs {
				if hasStats(log_) {
					if firstDone {
						b.WriteRune(',')
					}
					firstDone = true
					fmt.Fprintf(&b, "\"%s\":{\"gets\":%d,\"puts\":%d}",
						log_.name, log_.stats.getCount, log_.stats.putCount)
				}
			}
			fmt.Println(b.String())
			b.Reset()
		}
	}()

	return logsRoutine{c}
}

func findLog(logs []*msgLog, name string) *msgLog {
	for _, l := range logs {
		if l.name == name {
			return l
		}
	}
	return nil
}

/*    way/
 * create the requested db file with header.
 */
func createLogFile(dbloc, name string) error {
	loc := path.Join(dbloc, name)
	f, err := os.OpenFile(loc, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	f.Write([]byte(DBHeader))
	return nil
}

/*    way/
 * open the given event log file, read existing records, and set up a
 * goroutine to handle get and put requests
 */
func loadLog(dbloc, name string) (*msgLog, error) {
	loc := path.Join(dbloc, name)
	f, err := os.OpenFile(loc, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	msgs, err := readMsgs(f)
	if err != nil {
		return nil, err
	}
	nextnum := uint32(1)
	for _, msg := range msgs {
		if msg.num >= nextnum {
			nextnum = msg.num + 1
		}
	}

	var getCount uint32 = 0
	var putCount uint32 = 0

	g := make(chan getReq)
	p := make(chan putReq)
	s := make(chan statReq)
	go func() {
		for {
			select {
			case req := <-g:
				getCount++
				msgs_, err := get_(req.num, msgs, f)
				req.resp <- getReqResp{msgs_, err}
			case req := <-p:
				putCount++
				msg, err := put_(req.data, nextnum, f)
				if err != nil {
					req.resp <- putReqResp{err: err}
				} else {
					msgs = append(msgs, msg)
					nextnum = msg.num + 1
					req.resp <- putReqResp{num: msg.num}
				}
			case req := <-s:
				req.resp <- stats{getCount, putCount}
				getCount = 0
				putCount = 0
			}
		}
	}()

	return &msgLog{
		name: name,
		get:  g,
		put:  p,
		stat: s,
	}, nil
}

/*    way/
 * return a few messages (max 5 || size < 256) to the user
 */
func get_(num uint32, msgs []*msg, f *os.File) ([]*msg, error) {
	var msgs_ []*msg
	var tot uint32 = 0
	for i := 0; i < 5; i++ {
		ndx := num + uint32(i-1)
		if ndx < uint32(len(msgs)) {
			m := msgs[ndx]
			if m != nil {
				msg_, err := readMsg(*m, f)
				if err != nil {
					return nil, err
				}
				msgs_ = append(msgs_, msg_)
				tot += m.sz
			}
			if tot >= 256 {
				break
			}
		}
	}

	return msgs_, nil
}

/*    way/
 * read message data from disk
 */
func readMsg(msg_ msg, f *os.File) (*msg, error) {

	data := make([]byte, msg_.sz)
	_, err := f.ReadAt(data, msg_.offset)
	if err != nil {
		return nil, err
	}
	msg_.data = data

	return &msg_, nil
}

/*    way/
 * read in the message then append it to the end of the file with the
 * correct record header (KAF|num|sz)
 */
func put_(data []byte, num uint32, f *os.File) (*msg, error) {
	inf, err := f.Stat()
	if err != nil {
		return nil, err
	}
	off := inf.Size()

	sz := uint32(len(data))
	hdr := fmt.Sprintf("%s%d|%d%s", RecHeaderPfx, num, sz, RecHeaderSfx)
	hdr_ := []byte(hdr)
	if _, err := f.WriteAt(hdr_, off); err != nil {
		return nil, err
	}
	off += int64(len(hdr_))

	if _, err := f.WriteAt(data, off); err != nil {
		return nil, err
	}

	return &msg{
		offset: off,
		num:    num,
		sz:     sz,
		data:   nil,
	}, nil

}

/*    way/
 * Step through the file, loading message info (skipping the data)
 */
func readMsgs(f *os.File) ([]*msg, error) {
	dbhdr := []byte(DBHeader)
	hdr := make([]byte, len(dbhdr))
	if _, err := io.ReadFull(f, hdr); err != nil {
		return nil, err
	}
	if bytes.Compare(dbhdr, hdr) != 0 {
		return nil, errors.New("invalid db header")
	}

	var msgs []*msg
	inf, err := f.Stat()
	if err != nil {
		return nil, err
	}
	sz := inf.Size()
	offset := int64(len(DBHeader))
	for offset < sz {
		msg, err := readRecInfo(offset, f)
		if err != nil {
			return nil, err
		}
		if msg != nil && msg.num > 0 {
			msgs = append(msgs, msg)
		}
		if msg != nil && msg.sz != 0 {
			offset = msg.offset + int64(msg.sz)
		}
	}

	return msgs, nil
}

/*    way/
 * read a chunk of data from the offset that should be big enough to
 * hold the header (marked off by the newline) and return the message
 * info from the header.
 *
 *    understand/
 * message header is of the format:
 *    KAF|<string number>|<string size>\n
 */
func readRecInfo(off int64, f *os.File) (*msg, error) {
	const BIGENOUGH = 32
	hdr := make([]byte, BIGENOUGH)

	pos := struct {
		curr          int
		headerStart   int
		firstDivider  int
		secondDivider int
		headerEnd     int
	}{0, -1, -1, -1, -1}

	n, err := f.ReadAt(hdr, off)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	for ; pos.curr < n; pos.curr++ {
		if hdr[pos.curr] != '\n' {
			break
		}
	}

	if pos.curr == 0 {
		return nil, errors.New("invalid record header start")
	}

	if pos.curr == n {
		return &msg{
			offset: off,
			num:    0,
			sz:     uint32(n),
			data:   nil,
		}, nil
	}

	pos.headerStart = pos.curr - 1

	for ; pos.curr < n; pos.curr++ {
		if hdr[pos.curr] == '|' {
			if pos.firstDivider == -1 {
				pos.firstDivider = pos.curr
			} else if pos.secondDivider == -1 {
				pos.secondDivider = pos.curr
			} else {
				return nil, errors.New("invalid record header: extra '|' found")
			}
		}
		if hdr[pos.curr] == []byte(RecHeaderSfx)[0] {
			pos.headerEnd = pos.curr
			break
		}
	}

	rechdr := hdr[pos.headerStart : pos.firstDivider+1]
	if bytes.Compare(rechdr, []byte(RecHeaderPfx)) != 0 {
		return nil, errors.New("invalid record header prefix")
	}

	if pos.firstDivider == -1 {
		return nil, errors.New("invalid record header: no number")
	}

	if pos.secondDivider == -1 {
		return nil, errors.New("invalid record header: no size")
	}

	if pos.headerEnd == -1 {
		return nil, errors.New("invalid record header: not terminated correctly")
	}

	v := string(hdr[pos.firstDivider+1 : pos.secondDivider])
	num, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return nil, errors.New("invalid record header message number")
	}
	v = string(hdr[pos.secondDivider+1 : pos.headerEnd])
	sz, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return nil, errors.New("invalid record header message size")
	}

	return &msg{
		offset: off + int64(pos.headerEnd+1),
		num:    uint32(num),
		sz:     uint32(sz),
		data:   nil,
	}, nil

}

/*    way/
 * set up the request handlers and start the server
 */
func startServer(cfg *config, logsR logsRoutine) {
	setupRequestHandlers(cfg, logsR)

	log.Println("Starting server on", cfg.addr, "writing to", cfg.dbloc)
	log.Fatal(http.ListenAndServe(cfg.addr, nil))
}

/*    way/
 * set up our request handlers passing in the context and logs goroutine
 */
func setupRequestHandlers(cfg *config, lr logsRoutine) {
	wrapH := func(h reqHandler) httpHandler {
		return func(w http.ResponseWriter, r *http.Request) {
			h(cfg, r, lr, w)
		}
	}
	http.HandleFunc("/get/", wrapH(get))
	http.HandleFunc("/put/", wrapH(put))
}

/*    way/
 * helper function that requests logsRoutine for the given log
 */
func getLog(name string, logsR logsRoutine, create bool) (*msgLog, error) {
	c := make(chan logReqResp)
	logsR.c <- logReq{
		name:   name,
		create: create,
		resp:   c,
	}
	resp := <-c
	return resp.msglog, resp.err
}

/*    way/
 * handle /get/<logname>?from=num request, responding with messages from
 * the event log
 */
func get(cfg *config, r *http.Request, logsR logsRoutine, w http.ResponseWriter) {
	name := strings.TrimSpace(r.URL.Path[len("/get/"):])
	if len(name) == 0 {
		err_("Missing event log name", 400, r, w)
		return
	}

	qv := r.URL.Query()["from"]
	if qv == nil || len(qv) == 0 {
		err_("get: Missing 'from' message number", 400, r, w)
		return
	}
	num, err := strconv.ParseUint(qv[0], 10, 32)
	if err != nil || num < 1 {
		err_("get: Invalid 'from' message number", 400, r, w)
		return
	}

	msglog, err := getLog(name, logsR, false)
	if err != nil {
		err_(err.Error(), 500, r, w)
		return
	}

	var msgs []*msg
	if msglog != nil {
		c := make(chan getReqResp)
		msglog.get <- getReq{
			num:  uint32(num),
			resp: c,
		}
		resp := <-c
		if resp.err != nil {
			err_(resp.err.Error(), 500, r, w)
			return
		}
		msgs = resp.msgs
	}

	hdr := fmt.Sprintf("KAF|%d", len(msgs))
	if _, err := w.Write([]byte(hdr)); err != nil {
		err_("get: failed sending data back", 500, r, w)
		return
	}
	for _, m := range msgs {
		hdr := fmt.Sprintf("\nKAF|%d|%d\n", m.num, m.sz)
		if _, err := w.Write([]byte(hdr)); err != nil {
			err_("get: failed sending data back", 500, r, w)
			return
		}
		if _, err := w.Write(m.data); err != nil {
			err_("get: failed sending data back", 500, r, w)
			return
		}
	}
}

/*    way/
 * handle /put/<logname> request, responding with message number added
 * to event log
 */
func put(cfg *config, r *http.Request, logsR logsRoutine, w http.ResponseWriter) {
	name := strings.TrimSpace(r.URL.Path[len("/put/"):])
	if len(name) == 0 {
		err_("Missing event log name", 400, r, w)
		return
	}
	msglog, err := getLog(name, logsR, true)
	if err != nil {
		err_(err.Error(), 500, r, w)
		return
	}

	hsz := r.Header["Content-Length"]
	if len(hsz) == 0 {
		err_("put: No content-length found", 400, r, w)
		return
	}
	sz, err := strconv.ParseUint(hsz[0], 10, 32)
	if err != nil {
		err_("put: Invalid content-length", 400, r, w)
		return
	}
	if sz <= 0 {
		err_("put: Empty message length", 400, r, w)
		return
	}
	if sz > 5*1024*1024 {
		err_("put: too large message length", 400, r, w)
		return
	}

	data := make([]byte, sz)
	if data == nil {
		err_("put: Out of Memory", 500, r, w)
		return
	}
	if _, err := io.ReadFull(r.Body, data); err != nil {
		err_("put: failed reading message data", 400, r, w)
		return
	}

	c := make(chan putReqResp)
	msglog.put <- putReq{
		data: data,
		resp: c,
	}
	resp := <-c
	if resp.err != nil {
		err_(resp.err.Error(), 500, r, w)
		return
	}
	w.Write([]byte(strconv.FormatUint(uint64(resp.num), 10)))
}

/*    way/
 * respond with error helper function
 */
func err_(error string, code int, r *http.Request, w http.ResponseWriter) {
	log.Println("ERROR:", r.RemoteAddr, r.RequestURI, error)
	http.Error(w, error, code)
}

/* helper types */

type config struct {
	addr  string
	dbloc string
}

type reqHandler func(*config, *http.Request, logsRoutine, http.ResponseWriter)
type httpHandler func(http.ResponseWriter, *http.Request)
