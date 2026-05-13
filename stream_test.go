package gosse

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStream(t *testing.T) {
	Convey("Given a new stream", t, func() {
		// new stream
		str := newStream(1024)
		str.run()

		Convey("When adding a subscriber", func() {
			sub := str.addSubscriber()

			Convey("It should be stored", func() {
				So(len(str.subscribers), ShouldEqual, 1)
			})
			Convey("It should receive message", func() {
				str.event <- []byte("test")
				msg, err := wait(sub.connection, time.Second)

				So(err, ShouldBeNil)
				So(string(msg), ShouldEqual, "test")
			})
		})

		Convey("When removing a subscriber", func() {
			str.addSubscriber()
			str.removeSubscriber(0)
			Convey("It should be removed from the list of subscribers", func() {
				So(len(str.subscribers), ShouldEqual, 0)
			})
		})

		Convey("When closing a subscriber down gracefully", func() {
			sub := str.addSubscriber()
			sub.close()
			time.Sleep(time.Millisecond * 100)
			Convey("It should be removed from the list of subscribers", func() {
				So(len(str.subscribers), ShouldEqual, 0)
			})
		})

		Convey("When adding multiple subscribers", func() {
			var subs []*Subscriber
			for i := 0; i < 10; i++ {
				subs = append(subs, str.addSubscriber())
			}

			// Wait for all subscribers to be added
			time.Sleep(time.Millisecond * 100)

			Convey("They should all receive messages", func() {
				str.event <- []byte("test")
				for _, sub := range subs {
					msg, err := wait(sub.connection, time.Second*1)
					So(err, ShouldBeNil)
					So(string(msg), ShouldEqual, "test")
				}
			})

			Convey("They should all shutdown gracefully when the stream is closed", func() {
				str.close()

				// Wait for all subscribers to close
				time.Sleep(time.Millisecond * 100)

				So(len(str.subscribers), ShouldEqual, 0)
			})

		})
	})

}
