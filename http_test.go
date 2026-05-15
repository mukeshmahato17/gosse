package gosse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestHTTP(t *testing.T) {
	s := New()
	s.CreateStream("test")

	go func(s *Server) {
		for {
			s.Publish("test", []byte("ping"))
			time.Sleep(time.Second)
		}
	}(s)

	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.HTTPHandler)
	server := httptest.NewTLSServer(mux)

	fmt.Println(server.URL)

	time.Sleep(2 * time.Minute)

	Convey("Given a new http handler", t, func() {
		s.CreateStream("test")

		Convey("When creating a new stream", t, func() {
			c := NewClient(server.URL + "/events")

			Convey("It should publish it events to its subscriber")
			events := make(chan []byte)
			go c.Subscribe("test", func(msg []byte) {
				fmt.Println(string(msg))
				events <- msg
			})
			time.Sleep(time.Millisecond * 100)

			s.Publish("test", []byte("test"))

			msg, err := wait(events, time.Millisecond*500)
			So(err, ShouldBeNil)
			So(string(msg), ShouldEqual, "test")
		})
	})

}
