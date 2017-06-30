package adapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
	"strings"

	"github.com/gliderlabs/logspout/router"
)

const (
	logglyAddr          = "https://logs-01.loggly.com"
	logglyEventEndpoint = "/bulk"
	flushTimeout        = 10 * time.Second
)

// Adapter satisfies the router.LogAdapter interface by providing Stream which
// passes all messages to loggly.
type Adapter struct {
	bufferSize int
	log        *log.Logger
	logglyURL  string
	queue      chan logglyMessage
}

var debugFP, err = os.Create("/tmp/debug.log")

// New returns an Adapter that receives messages from logspout. Additionally,
// it launches a goroutine to buffer and flush messages to loggly.
func New(logglyToken string, tags string, bufferSize int) *Adapter {
	adapter := &Adapter{
		bufferSize: bufferSize,
		log:        log.New(os.Stdout, "logspout-loggly", log.LstdFlags),
		logglyURL:  buildLogglyURL(logglyToken, tags),
		queue:      make(chan logglyMessage),
	}

	debugFP.WriteString("created a new adapter\n")

	go adapter.readQueue()

	return adapter
}

// Stream satisfies the router.LogAdapter interface and passes all messages to
// Loggly
func (l *Adapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		l.queue <- logglyMessage{
			Message:           m.Data,
			ContainerName:     m.Container.Name,
			ContainerID:       m.Container.ID,
			ContainerImage:    m.Container.Config.Image,
			ContainerHostname: m.Container.Config.Hostname,
		}
	}
}

func (l *Adapter) readQueue() {
	buffer := l.newBuffer()

	timeout := time.NewTimer(flushTimeout)

	for {
		select {
		case msg := <-l.queue:
			if len(buffer) == cap(buffer) {
				timeout.Stop()
				l.flushBuffer(buffer)
				buffer = l.newBuffer()
			}

			buffer = append(buffer, msg)

		case <-timeout.C:
			if len(buffer) > 0 {
				l.flushBuffer(buffer)
				buffer = l.newBuffer()
			}
		}

		timeout.Reset(flushTimeout)
	}
}

func (l *Adapter) newBuffer() []logglyMessage {
	return make([]logglyMessage, 0, l.bufferSize)
}

func (l *Adapter) flushBuffer(buffer []logglyMessage) {
	var dataBuffers = make(map[string] bytes.Buffer, 5)

	debugFP.WriteString("Flushing the buffer")

	for _, msg := range buffer {
		var logglyURL = addTagsToLogglyURL(l.logglyURL, msg.ContainerName)
		debugFP.WriteString("** Container name: " + msg.ContainerName)
		if _, ok := dataBuffers[logglyURL]; !ok {
			debugFP.WriteString(logglyURL + " did not exist.  Allocating...")
			dataBuffers[logglyURL] = bytes.Buffer{}
		} else {
			debugFP.WriteString(logglyURL + " was already setup")
		}
		data := dataBuffers[logglyURL]
		j, _ := json.Marshal(msg)
		data.Write(j)
		data.WriteString("\n")
	}

	//for containerName, data := range dataBuffers {
	for url, data := range dataBuffers {
		req, _ := http.NewRequest(
			"POST",
			url,
			&data,
		)
		go l.sendRequestToLoggly(req)
	}
}

func (l *Adapter) sendRequestToLoggly(req *http.Request) {
	resp, err := http.DefaultClient.Do(req)

	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		l.log.Println(
			fmt.Errorf(
				"error from client: %s",
				err.Error(),
			),
		)
		return
	}

	if resp.StatusCode != http.StatusOK {
		l.log.Println(
			fmt.Errorf(
				"received a %s status code when sending message. response: %s",
				resp.StatusCode,
				resp.Body,
			),
		)
	}
}

func buildLogglyURL(token, tags string) string {
	var url string
	url = fmt.Sprintf(
		"%s%s/%s",
		logglyAddr,
		logglyEventEndpoint,
		token,
	)

	return addTagsToLogglyURL(url, tags)
}

func addTagsToLogglyURL(url, tags string) string {
	const sep  = "/tag/"

	if tags == "" {
		return url
	}

	if i := strings.Index(url,sep); i > 0 {
		i += len(sep)
		url = string(append([]byte(url)[:i], append([]byte(tags + ","), []byte(url)[i:]...)...))
	} else {
		url = fmt.Sprintf(
			"%s/tag/%s/",
			url,
			tags,
		)
	}

	fmt.Printf("Added %s to %s", tags, url);

	return url
}
