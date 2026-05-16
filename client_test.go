package gosse

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

var url string

func setup() {
	s := New()

	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.HTTPHandler)
	server := httptest.NewServer(mux)
	url = server.URL + "/events"

	s.CreateStream("stream")

	// Send continuous string of events to client
	go func(s *Server) {
		for {
			s.Publish("test", []byte("ping"))
			time.Sleep(500 * time.Millisecond)
		}
	}(s)
}

func TestClient(t *testing.T) {
	setup()

	Convey("Given a new client", t, func() {
		c := NewClient(url)

		Convey("When connecting to a new stream", func() {
			Convey("It should receive events", func() {
				events := make(chan []byte)
				var cErr error
				go func(cErr error) {
					cErr = c.Subscribe("test", func(msg *Event) {
						if msg.Data != nil {
							events <- msg.Data
							return
						}
					})
				}(cErr)

				for i := 0; i < 5; i++ {
					msg, err := wait(events, time.Second)
					So(err, ShouldBeNil)
					So(string(msg), ShouldEqual, "ping")
				}
				So(cErr, ShouldBeNil)
			})
		})
	})
}
