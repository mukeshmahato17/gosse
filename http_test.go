package gosse

import (
	"context"
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
	defer server.Close()

	Convey("Given a new http handler", t, func() {
		s.CreateStream("test")

		Convey("When creating a new stream", func() {
			c := NewClient(server.URL + "/events")

			Convey("It should publish its events to its subscriber", func() {
				events := make(chan []byte, 10)
				errChan := make(chan error, 1)

				subCtx, cancelSub := context.WithCancel(context.Background())
				defer cancelSub()

				go func() {
					err := c.Subscribe(subCtx, "test", func(msg *Event) {
						if msg != nil && msg.Data != nil {
							events <- msg.Data
						}
					})
					if err != nil {
						errChan <- err
					}
				}()

				// Continually broadcast until the connection catches it
				stopTicker := make(chan struct{})
				go func() {
					ticker := time.NewTicker(50 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stopTicker:
							return
						case <-ticker.C:
							s.Publish("test", []byte("ping"))
						}
					}
				}()

				select {
				case msg := <-events:
					So(string(msg), ShouldEqual, "ping")
				case err := <-errChan:
					t.Fatalf("subscriber encountered an error: %v", err)
				case <-time.After(2 * time.Second):
					t.Fatal("timeout waiting for stream event")
				}

				close(stopTicker)
				cancelSub()
			})
		})
	})
}
