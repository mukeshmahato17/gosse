package gosse

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestHTTP(t *testing.T) {
	s := New()
	defer s.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.HTTPHandler)
	server := httptest.NewServer(mux)

	Convey("Given a new http handler", t, func() {
		s.CreateStream("test")

		Convey("When creating a new stream", func() {
			c := NewClient(server.URL + "/events")

			Convey("It should publish it events to its subscriber", func() {
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

				// Wait for the subscribr to be register and message to be published
				time.Sleep(time.Millisecond * 200)
				s.Publish("test", []byte("test"))

				msg, err := wait(events, time.Millisecond*500)
				So(err, ShouldBeNil)
				So(string(msg), ShouldEqual, "test")

			})
		})
	})
}
