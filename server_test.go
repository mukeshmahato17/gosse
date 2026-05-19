package gosse

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func wait(ch chan []byte, duration time.Duration) ([]byte, error) {
	var (
		err error
		msg []byte
	)

	select {
	case event := <-ch:
		msg = event
	case <-time.After(duration):
		err = errors.New("timeout")
	}

	return msg, err
}

func TestServer(t *testing.T) {
	// new server
	server := New()

	Convey("Given a new server", t, func() {
		Convey("When creating a new stream", func() {
			server.CreateStream("test")

			Convey("It should be stored", func() {
				So(server.getStream("test"), ShouldNotBeNil)
			})
			Convey("It should be started", func() {
			})
		})

		Convey("When removing a stream", func() {
			server.CreateStream("test")
			server.RemoveStream("test")

			Convey("It should be removed", func() {
				So(server.getStream("test"), ShouldBeNil)
			})
		})

		Convey("When publishing to a stream that already exists", func() {
			server.CreateStream("test")
			stream := server.getStream("test")
			sub := stream.addSubscriber()

			server.Publish("test", []byte("test"))
			Convey("It must be received by the subscribers", func() {
				msg, err := wait(sub.connection, time.Second)
				So(err, ShouldBeNil)
				So(string(msg), ShouldEqual, "test")
			})
		})

		Convey("when publishing to a stream that doesnot exists", func() {
			server.Publish("test", []byte("test"))
			Convey("It must not cause an error", func() {
				So(func() { server.Publish("test", []byte("test")) }, ShouldNotPanic)
			})
		})
	})
}
