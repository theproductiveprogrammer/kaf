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
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

/*    understand/
 * program version
 */
const VERSION = "1.5.3"

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
 * represents a request for a particular log routine from the main
 * logsRoutine goroutine. We can request for the log to be created if it
 * doesn't exist and we expect our response to be sent back via the
 * channel we provide (either the log itself or an error)
 */
type logReq struct {
	name   string
	create bool

	resp chan logReqResp
}
type logReqResp struct {
	logR *logRoutine
	err  error
}

/*    understand/
 * represents a request for all log routines currently being managed.
 */
type allLogsReq struct {
	resp chan []*logRoutine
}

/*    understand/
 * similar to logsRoutine, each message log is also handled by it's own
 * goroutine. We communicate to it via it's channels - either to get
 * message logs or to put a new message log or get info.
 */
type logRoutine struct {
	name string
	get  chan getReq
	put  chan putReq
	ach  chan archiveReq
	stat chan statReq
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
 * represents a request to a message log archive the log and continue
 * with a new log.
 */
type archiveReq struct {
	upto uint32
	resp chan achReqResp
}
type achReqResp struct {
	err error
}

/*    understand/
 * represents a request to a message log get stats
 */
type statReq struct {
	resp chan stats
}

/*    understand/
 * represents a message in the event log
 */
type msg struct {
	offset int64
	start  uint32
	num    uint32
	sz     uint32
	data   []byte
}

/*    understand/
 * hold an offset to the message in the message log so it's easy to get
 * to and read
 */
type msgOff struct {
	num    uint32
	offset int64
}

/*    understand/
 * important info on the message log
 */
type msgLog struct {
	name    string
	loc     string
	f       *os.File
	size    int64
	lastmsg uint32
	msgOs   []msgOff

	getCount uint32
	putCount uint32
	achCount uint32
	errCount uint32
}

/*    understand/
 * relevant stats for a message log are available in the msgLog
 * structure
 */
type stats msgLog

/*
 * Data File constants
 */
const DBHeader = "KAF_DB|v1|"
const RecHeaderPfx = "\nKAF_MSG|"
const RecHeaderSfx = "\n"
const RespHeaderPfx = "KAF_MSGS|v1"

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
 * start up the goroutine, load all logs from disk, and set up the stat tracker
 */
func getLogsRoutine(dbloc string) logsRoutine {

	c := make(chan logReq)
	a := make(chan allLogsReq)
	go logsGo(dbloc, c, a)

	logsR := logsRoutine{c}

	err := loadAllLogs(dbloc, logsR)
	if err != nil {
		log.Println(err)
		log.Panic("Failed loading all logs from", dbloc)
	}

	go statsGo(logsR, a)

	return logsR
}

/*    understand/
 * manages all log routines, handling creating new routines and
 * returning routines as requested
 */
func logsGo(dbloc string, c chan logReq, a chan allLogsReq) {
	var logRs []*logRoutine
	for {
		select {
		case req := <-c:
			logR := findLogR(logRs, req.name)
			if logR != nil {
				req.resp <- logReqResp{logR, nil}
				continue
			}

			loc := path.Join(dbloc, req.name)

			if req.create && !fileExists(loc) {
				createLogFile(loc, 0)
			}

			if fileExists(loc) {

				logR, err := loadLogR(req.name, loc)
				if err != nil {
					req.resp <- logReqResp{nil, err}
				} else {
					logRs = append(logRs, logR)
					req.resp <- logReqResp{logR, nil}
				}
			} else {

				req.resp <- logReqResp{}
			}

		case req := <-a:
			req.resp <- logRs
		}
	}
}

/*    way/
 * load all existing logs from disk
 */
func loadAllLogs(dbloc string, logsR logsRoutine) error {
	files, err := ioutil.ReadDir(dbloc)
	if err != nil {
		return err
	}

	for _, f := range files {
		if isHidden(f.Name()) {
			continue
		}
		_, err := getLog(f.Name(), logsR, true)
		if err != nil {
			return errors.New(fmt.Sprintf("%s: %s", f.Name(), err.Error()))
		}
	}

	return nil
}

/*    way/
 * periodically post statistics of all logs that have activity
 */
func statsGo(logsR logsRoutine, a chan allLogsReq) {
	ticker := time.NewTicker(5 * time.Minute)
	c := make(chan stats)
	r := make(chan []*logRoutine)
	var b strings.Builder

	var statCount uint32 = 0
	for {
		start := time.Now()

		<-ticker.C
		statCount++

		allstats := []stats{}
		a <- allLogsReq{r}
		for _, logR := range <-r {
			logR.stat <- statReq{resp: c}
			stats := <-c
			if stats.name != "_kaf" && hasActivity(stats) {
				allstats = append(allstats, stats)
			}
		}

		end := time.Now()

		if len(allstats) == 0 {
			continue
		}

		logR, err := getLog("_kaf", logsR, true)
		if err != nil {
			log.Println(err)
			continue
		}

		statsJSON(allstats, statCount, start, end, &b)

		c := make(chan putReqResp)
		logR.put <- putReq{
			data: []byte(b.String()),
			resp: c,
		}
		resp := <-c
		if resp.err != nil {
			log.Println(err)
		}

	}
}

/*    understand/
 * ignore empty filenames, dot files, and 'archived' files i(those
 * starting with --)
 */
func isHidden(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[0] == '.' {
		return true
	}
	if strings.HasPrefix(name, "--") {
		return true
	}
	return false
}

/*    understand/
 * we count a log as having activity if it has any get or put requests
 * or any errors
 */
func hasActivity(stats stats) bool {
	return stats.getCount+stats.putCount > 0 || stats.errCount > 0
}

/*    way/
 * convert all the stats received to a JSON report
 */
func statsJSON(allstats []stats,
	statCount uint32, start, end time.Time,
	b *strings.Builder) {

	b.Reset()

	b.WriteString(`{"start":"`)
	b.WriteString(start.UTC().Format(time.RFC3339))
	b.WriteString(`","end":"`)
	b.WriteString(end.UTC().Format(time.RFC3339))
	fmt.Fprintf(b, `","statno":%d,"logs":[`, statCount)

	for i, stats := range allstats {

		if stats.errCount > 0 {
			fmt.Fprintf(b,
				`{"name":"%s","last":%d,"gets":%d,"puts":%d,"errs":%d}`,
				stats.name,
				stats.lastmsg,
				stats.getCount, stats.putCount,
				stats.errCount)
		} else if hasActivity(stats) {
			fmt.Fprintf(b,
				`{"name":"%s","last":%d,"gets":%d,"puts":%d}`,
				stats.name,
				stats.lastmsg,
				stats.getCount, stats.putCount)
		} else {
			fmt.Fprintf(b,
				`{"name":"%s","last":%d}`,
				stats.name,
				stats.lastmsg)
		}

		if i != len(allstats)-1 {
			b.WriteRune(',')
		}
	}

	b.WriteString("]}")
}

func fileExists(loc string) bool {
	info, err := os.Stat(loc)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func findLogR(logRs []*logRoutine, name string) *logRoutine {
	for _, l := range logRs {
		if l.name == name {
			return l
		}
	}
	return nil
}

/*    way/
 * create the requested db file with header.
 */
func createLogFile(loc string, lastmsg uint32) error {
	f, err := os.OpenFile(loc, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	no := strconv.FormatUint(uint64(lastmsg), 10)
	f.Write([]byte(DBHeader))
	f.Write([]byte(no))
	return nil
}

/*    way/
 * load records from the log file and, and set up a goroutine to handle requests
 */
func loadLogR(name, loc string) (*logRoutine, error) {
	msglog := &msgLog{
		name: name,
		loc:  loc,
	}
	err := loadLogFile(msglog)
	if err != nil {
		return nil, err
	}

	g := make(chan getReq)
	p := make(chan putReq)
	s := make(chan statReq)
	a := make(chan archiveReq)
	go func() {
		for {
			select {
			case req := <-g:
				req.resp <- get_(req.num, msglog)
			case req := <-p:
				req.resp <- put_(req.data, msglog)
			case req := <-a:
				req.resp <- archive_(req.upto, msglog)
			case req := <-s:
				stats := stats(*msglog)
				msglog.getCount = 0
				msglog.putCount = 0
				msglog.achCount = 0
				msglog.errCount = 0

				req.resp <- stats
			}
		}
	}()

	return &logRoutine{
		name: name,
		get:  g,
		put:  p,
		ach:  a,
		stat: s,
	}, nil
}

/*    way/
 * keep track of the message offset from which we want to copy,
 * close/clean the existing message log, rename the existing file,
 * create a new log file, copy any existing messages and reload it.
 */
func archive_(upto uint32, msglog *msgLog) achReqResp {
	msglog.achCount++

	if len(msglog.msgOs) == 0 {
		return achReqResp{errors.New("empty logfile: nothing toarchive")}
	}
	if upto == 0 {
		return achReqResp{errors.New("message to archive upto not given")}
	}

	ndx := findMsgNdx(msglog.msgOs, upto)
	if ndx >= uint32(len(msglog.msgOs)) {
		upto = msglog.lastmsg
	}

	var firstMsg msgOff
	if ndx < uint32(len(msglog.msgOs)) {
		firstMsg = msglog.msgOs[ndx]
		if firstMsg.num == upto {
			firstMsg.num = 0
			firstMsg.offset = 0
			ndx++
		}
		if ndx < uint32(len(msglog.msgOs)) {
			firstMsg = msglog.msgOs[ndx]
		}
	}

	clearMsgLog(msglog)

	t := time.Now().UTC().Format("2006-01-02T15_04_05Z07_00")
	aname := fmt.Sprintf("--%s--%s", msglog.name, t)
	aloc := filepath.Join(filepath.Dir(msglog.loc), aname)
	if err := os.Rename(msglog.loc, aloc); err != nil {
		msglog.errCount++
		return achReqResp{err}
	}

	createLogFile(msglog.loc, upto)
	src, err := os.OpenFile(aloc, os.O_RDWR, 0644)
	if err != nil {
		return achReqResp{err}
	}
	defer src.Close()
	dst, err := os.OpenFile(msglog.loc, os.O_RDWR, 0644)
	if err != nil {
		return achReqResp{err}
	}
	defer dst.Close()

	if firstMsg.offset > 0 {
		if _, err := src.Seek(firstMsg.offset, 0); err != nil {
			return achReqResp{err}
		}
		if _, err := dst.Seek(0, 2); err != nil {
			return achReqResp{err}
		}
		buf := make([]byte, 4096)
		for {
			n, err := src.Read(buf)
			if err != nil && err != io.EOF {
				return achReqResp{err}
			}
			if n == 0 {
				break
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return achReqResp{err}
			}
		}
	}

	return achReqResp{loadLogFile(msglog)}
}

/*    problem/
 * return a few messages (max 5 || size < 3200) to the user
 *    way/
 * find the index of the first message >= the number and then walk the
 * next few messages, stopping when too big or out of bounds
 * NB: Why 3200? We want sizes to be small enough so they fit the
 * initial congestion window of TCP - we could probably go (much?) higher
 * but we don't expect large data records anyway so 3200 is reasonable.
 */
func get_(num uint32, msglog *msgLog) getReqResp {
	msglog.getCount++

	ndx := findMsgNdx(msglog.msgOs, num)

	var msgs []*msg
	var i, tot, l uint32
	l = uint32(len(msglog.msgOs))
	for ; i < 5 && ndx+i < l; i++ {
		mo := msglog.msgOs[ndx+i]
		msg, err := readMsg(mo, msglog.f)
		if err != nil {
			msglog.errCount++
			return getReqResp{nil, err}
		}
		msgs = append(msgs, msg)
		tot += msg.sz
		if tot >= 3200 {
			break
		}
	}

	return getReqResp{msgs, nil}
}

/*    way/
 * binary search for first index that matches the number passed in
 */
func findMsgNdx(a []msgOff, num uint32) uint32 {
	if len(a) == 0 {
		return 0
	}
	s := uint32(0)
	e := uint32(len(a) - 1)
	for s <= e {
		if num <= a[s].num {
			return s
		}
		if a[e].num < num {
			break
		}
		m := (s + e) / 2
		if m == s {
			return e
		}
		if a[m].num < num {
			s = m
		} else {
			e = m
		}
	}
	return uint32(len(a))
}

/*    way/
 * validate that message header is correct then,
 * read message data from disk
 */
func readMsg(mo msgOff, f *os.File) (*msg, error) {

	msg, err := readRecInfo(mo.offset, f)
	if err != nil {
		return nil, err
	}

	if msg.num == 0 {
		m := fmt.Sprintf("No message data found at offset %d for msg %d", mo.offset, mo.num)
		return nil, errors.New(m)
	}

	if mo.num != msg.num {
		return nil, errors.New("Message number on disk incorrect")
	}

	data := make([]byte, msg.sz)
	_, err = f.ReadAt(data, msg.offset+int64(msg.start))
	if err != nil {
		return nil, err
	}
	msg.data = data

	return &msg, nil
}

/*    way/
 * read in the message then append it to the end of the file with the
 * correct record header (KAF|num|sz)
 */
func put_(data []byte, msglog *msgLog) putReqResp {
	msglog.putCount++

	inf, err := msglog.f.Stat()
	if err != nil {
		msglog.errCount++
		return putReqResp{0, err}
	}
	if msglog.size != inf.Size() {
		if !fileExists(msglog.loc) {
			createLogFile(msglog.loc, 0)
		}
		if err := loadLogFile(msglog); err != nil {
			return putReqResp{0, err}
		}
	}
	off := inf.Size()
	num := msglog.lastmsg + 1

	hdr := fmt.Sprintf("%s%d|%d%s", RecHeaderPfx, num, len(data), RecHeaderSfx)
	hdr_ := []byte(hdr)
	if _, err := msglog.f.WriteAt(hdr_, off); err != nil {
		msglog.errCount++
		return putReqResp{0, err}
	}
	start := uint32(len(hdr_))

	if _, err := msglog.f.WriteAt(data, off+int64(start)); err != nil {
		msglog.errCount++
		return putReqResp{0, err}
	}

	msglog.msgOs = append(msglog.msgOs, msgOff{num, off})
	msglog.lastmsg = num
	msglog.size += int64(len(data)) + int64(start)

	return putReqResp{num, nil}
}

/*    outcome/
 * clear any existing data, (re)-open the log file, read in the header
 * and message offsets and repopulate the msglog
 */
func loadLogFile(msglog *msgLog) error {
	clearMsgLog(msglog)

	f, err := os.OpenFile(msglog.loc, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	inf, err := f.Stat()
	if err != nil {
		return err
	}

	msglog.f = f
	msglog.size = inf.Size()

	hdrEnd, err := loadDBHeader(msglog)
	if err != nil {
		return err
	}

	if err := loadMsgOffsets(hdrEnd, msglog); err != nil {
		return err
	}

	return nil
}

func clearMsgLog(msglog *msgLog) {
	if msglog.f != nil {
		msglog.f.Close()
	}
	msglog.f = nil
	msglog.size = 0
	msglog.lastmsg = 0
	msglog.msgOs = nil
}

/*    outcome/
 * validate the first part of the header (fixed part), then read in the
 * lastmsg number
 */
func loadDBHeader(msglog *msgLog) (int64, error) {
	const BIGENOUGH = 32
	hdr := make([]byte, BIGENOUGH)

	n, err := msglog.f.ReadAt(hdr, 0)
	if err != nil && err != io.EOF {
		return 0, err
	}

	l := len(DBHeader)

	if bytes.Compare([]byte(DBHeader), hdr[:l]) != 0 {
		return 0, errors.New("invalid db header")
	}

	e := l
	for e < n {
		e++
		if e == n || hdr[e] == '\n' {
			break
		}
	}

	lastmsg, err := strconv.ParseUint(string(hdr[l:e]), 10, 32)
	if err != nil {
		m := fmt.Sprintf("bad last message number: %s", hdr[l:e])
		return 0, errors.New(m)
	}

	msglog.lastmsg = uint32(lastmsg)

	return int64(e), nil
}

/*    way/
 * Step through the file, loading message offsets
 */
func loadMsgOffsets(start int64, msglog *msgLog) error {
	offset := start

	var msgOs []msgOff
	for offset < msglog.size {
		msg, err := readRecInfo(offset, msglog.f)
		if err != nil {
			return err
		}
		if msg.num > 0 {
			msgOs = append(msgOs, msgOff{msg.num, msg.offset})
			if msg.num <= msglog.lastmsg {
				m := fmt.Sprintf("message number did not increase (%d !< %d)", msglog.lastmsg, msg.num)
				return errors.New(m)
			}
			msglog.lastmsg = msg.num
		}
		if msg.sz+msg.start != 0 {
			offset = msg.offset + int64(msg.start+msg.sz)
		}
	}

	msglog.msgOs = msgOs

	return nil
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
func readRecInfo(off int64, f *os.File) (msg, error) {
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
		return msg{}, err
	}

	if n == 0 {
		m := fmt.Sprintf("read at offset %d failed", off)
		return msg{}, errors.New(m)
	}

	for ; pos.curr < n; pos.curr++ {
		if hdr[pos.curr] != '\n' {
			break
		}
	}

	if pos.curr == 0 {
		return msg{}, errors.New("invalid record header start")
	}

	if pos.curr == n {
		return msg{
			offset: off,
			start:  uint32(n),
			num:    0,
			sz:     0,
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
				return msg{}, errors.New("invalid record header: extra '|' found")
			}
		}
		if hdr[pos.curr] == []byte(RecHeaderSfx)[0] {
			pos.headerEnd = pos.curr
			break
		}
	}

	rechdr := hdr[pos.headerStart : pos.firstDivider+1]
	if bytes.Compare(rechdr, []byte(RecHeaderPfx)) != 0 {
		return msg{}, errors.New("invalid record header prefix")
	}

	if pos.firstDivider == -1 {
		return msg{}, errors.New("invalid record header: no number")
	}

	if pos.secondDivider == -1 {
		return msg{}, errors.New("invalid record header: no size")
	}

	if pos.headerEnd == -1 {
		return msg{}, errors.New("invalid record header: not terminated correctly")
	}

	v := string(hdr[pos.firstDivider+1 : pos.secondDivider])
	num, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return msg{}, errors.New("invalid record header message number")
	}
	v = string(hdr[pos.secondDivider+1 : pos.headerEnd])
	sz, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return msg{}, errors.New("invalid record header message size")
	}

	return msg{
		offset: off,
		start:  uint32(pos.headerEnd + 1),
		num:    uint32(num),
		sz:     uint32(sz),
		data:   nil,
	}, nil

}

/*    way/
 * setup the server with the correct configuration and handlers and
 * start it up
 */
func startServer(cfg *config, logsR logsRoutine) {

	s := &http.Server{
		Addr:           cfg.addr,
		Handler:        requestHandlers(cfg, logsR),
		ReadTimeout:    time.Second,
		WriteTimeout:   time.Second,
		MaxHeaderBytes: 4096,
	}

	log.Println("Starting server on", cfg.addr, "writing to", cfg.dbloc)
	log.Fatal(s.ListenAndServe())
}

/*    way/
 * return a mux with our request handlers
 */
func requestHandlers(cfg *config, lr logsRoutine) *http.ServeMux {
	wrapH := func(h reqHandler) httpHandler {
		return func(w http.ResponseWriter, r *http.Request) {
			h(cfg, r, lr, w)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/get/", wrapH(get))
	mux.HandleFunc("/put/", wrapH(put))
	mux.HandleFunc("/archive/", wrapH(archive))
	return mux
}

/*    way/
 * helper function that requests logsRoutine for the given log
 */
func getLog(name string, logsR logsRoutine, create bool) (*logRoutine, error) {
	c := make(chan logReqResp)
	logsR.c <- logReq{
		name:   name,
		create: create,
		resp:   c,
	}
	resp := <-c
	return resp.logR, resp.err
}

/*    way/
 * handle /get/<logname>?from=num&format=[kaf|raw|json] request, responding
 * with messages from the event log
 */
func get(cfg *config, r *http.Request, logsR logsRoutine, w http.ResponseWriter) {
	name := strings.TrimSpace(r.URL.Path[len("/get/"):])
	if isHidden(name) {
		err_("get: invalid log name", 400, r, w)
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

	logR, err := getLog(name, logsR, false)
	if err != nil {
		err_(err.Error(), 500, r, w)
		return
	}

	var msgs []*msg
	if logR != nil {
		c := make(chan getReqResp)
		logR.get <- getReq{
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

	if len(msgs) > 0 {
		lastmsg := msgs[len(msgs)-1].num
		w.Header().Add("X-Kaf-LastMsgSent", strconv.FormatUint(uint64(lastmsg), 10))
	}

	format := "kaf"
	qv = r.URL.Query()["format"]
	if len(qv) > 0 {
		format = qv[0]
	}
	switch format {
	case "raw":
		rawFormat(msgs, r, w)
	case "json":
		jsonFormat(msgs, r, w)
	default:
		kafFormat(msgs, r, w)
	}
}

/*    way/
 * respond in kaf format - with headers and data - setting the content
 * type and content length for efficiency.
 */
func kafFormat(msgs []*msg, r *http.Request, w http.ResponseWriter) {
	respHdr := fmt.Sprintf("%s|%d", RespHeaderPfx, len(msgs))
	respSz := len(respHdr)
	msgHdrs := make([][]byte, len(msgs))
	for i, m := range msgs {
		msgHdrs[i] = []byte(fmt.Sprintf("%s%d|%d\n", RecHeaderPfx, m.num, m.sz))
		respSz += len(msgHdrs[i])
		respSz += len(m.data)
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", strconv.FormatUint(uint64(respSz), 10))

	if _, err := w.Write([]byte(respHdr)); err != nil {
		err_("get: failed sending data back", 500, r, w)
		return
	}
	for i, m := range msgs {
		hdr := msgHdrs[i]
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
 * respond in raw format - just data without headers - setting the content
 * type and content length for efficiency.
 */
func rawFormat(msgs []*msg, r *http.Request, w http.ResponseWriter) {

	respSz := 0
	for _, m := range msgs {
		respSz += len(m.data)
		respSz += len("\n")
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", strconv.FormatUint(uint64(respSz), 10))

	for _, m := range msgs {
		if _, err := w.Write(m.data); err != nil {
			err_("get: failed sending data back", 500, r, w)
			return
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			err_("get: failed sending data back", 500, r, w)
			return
		}
	}
}

/*    way/
 * respond in json format - making the assumption that all stored data
 * is json we wrap it in a JSON array and set the content
 * type (and content length for efficiency).
 */
func jsonFormat(msgs []*msg, r *http.Request, w http.ResponseWriter) {

	respSz := len("[]")
	for i, m := range msgs {
		if i != 0 {
			respSz += len(",\n")
		}
		respSz += len(m.data)
	}

	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("Content-Length", strconv.FormatUint(uint64(respSz), 10))

	goterr := false
	wr := func(d []byte) {
		if goterr {
			return
		}
		if _, err := w.Write(d); err != nil {
			goterr = true
			err_("get: failed sending data back", 500, r, w)
		}
	}

	wr([]byte("["))
	for i, m := range msgs {
		if i != 0 {
			wr([]byte(",\n"))
		}
		wr(m.data)
	}
	wr([]byte("]"))
}

/*    way/
 * handle /put/<logname> request, responding with message number added
 * to event log
 */
func put(cfg *config, r *http.Request, logsR logsRoutine, w http.ResponseWriter) {
	name := strings.TrimSpace(r.URL.Path[len("/put/"):])
	if isHidden(name) {
		err_("put: invalid log name", 400, r, w)
		return
	}
	logR, err := getLog(name, logsR, true)
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
	logR.put <- putReq{
		data: data,
		resp: c,
	}
	resp := <-c
	if resp.err != nil {
		err_(resp.err.Error(), 500, r, w)
		return
	}

	w.Header().Add("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(strconv.FormatUint(uint64(resp.num), 10)))
}

/*    way/
 * handle /archive/<logname>?upto=num request
 */
func archive(cfg *config, r *http.Request, logsR logsRoutine, w http.ResponseWriter) {
	name := strings.TrimSpace(r.URL.Path[len("/archive/"):])
	if isHidden(name) {
		err_("archive: invalid log name", 400, r, w)
		return
	}

	qv := r.URL.Query()["upto"]
	if qv == nil || len(qv) == 0 {
		err_("archive: Missing 'upto' message number", 400, r, w)
		return
	}
	num, err := strconv.ParseUint(qv[0], 10, 32)
	if err != nil || num < 1 {
		err_("archive: Invalid 'upto' message number", 400, r, w)
		return
	}

	logR, err := getLog(name, logsR, false)
	if err != nil || logR == nil {
		err_("archive: Invalid log", 400, r, w)
		return
	}

	c := make(chan achReqResp)
	logR.ach <- archiveReq{
		upto: uint32(num),
		resp: c,
	}
	resp := <-c
	if resp.err != nil {
		err_(resp.err.Error(), 500, r, w)
		return
	}
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
