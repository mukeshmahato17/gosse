package gosse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestClient(t *testing.T) {
	// Initialize server components cleanly inside the test
	s := New()
	s.CreateStream("test") // Match the stream name we intend to use

	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.HTTPHandler)
	server := httptest.NewServer(mux)
	defer server.Close() // Good practice to prevent leaking ports

	testURL := server.URL + "/events"

	// Start publishing mock data in the background
	stopPublish := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopPublish:
				return
			default:
				// Safely publish to the stream we actually created
				s.Publish("test", []byte("ping\n"))
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	defer close(stopPublish)

	Convey("Given a new client", t, func() {
		c := NewClient(testURL)

		Convey("When connecting to a new stream", func() {
			Convey("It should receive events", func() {
				events := make(chan []byte, 10)
				errChan := make(chan error, 1)

				// Create a cancelable context context for our subscriber lifecycle
				subCtx, cancelSub := context.WithCancel(context.Background())
				defer cancelSub() // Safeguard cleanup

				// Run subscription in background passing the context
				go func() {
					err := c.Subscribe(subCtx, "test", func(msg *Event) {
						if msg != nil && len(msg.Data) > 0 {
							events <- msg.Data
						}
					})
					if err != nil {
						errChan <- err
					}
				}()

				// Verify we can pull 5 sequential pings safely
				for i := 0; i < 5; i++ {
					select {
					case msg := <-events:
						So(string(msg), ShouldEqual, "ping")
					case err := <-errChan:
						t.Fatalf("Subscription failed unexpectedly: %v", err)
					case <-time.After(2 * time.Second):
						t.Fatal("Timeout waiting for stream event")
					}
				}

				// CRITICAL FIX: Kill the subscriber connection loop right here!
				cancelSub()
			})
		})
	})
}
