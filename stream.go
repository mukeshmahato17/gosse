package gosse

import "fmt"

type Stream struct {
	subscribers []*Subscriber
	register    chan *Subscriber
	unregister  chan *Subscriber
	event       chan []byte
	quit        chan bool
}

func NewStream(bufSize int) *Stream {
	return &Stream{
		subscribers: make([]*Subscriber, 0),
		register:    make(chan *Subscriber),
		unregister:  make(chan *Subscriber),
		event:       make(chan []byte, bufSize),
		quit:        make(chan bool),
	}
}

func (str *Stream) NewSubscriber() *Subscriber {
	sub := &Subscriber{
		Quit:       str.unregister,
		Connection: make(chan []byte),
	}

	str.register <- sub
	return sub
}

func (str *Stream) Publish(event []byte) {
	str.event <- event
}

func (str *Stream) run() {
	go func(s *Stream) {
		for {
			fmt.Println(len(s.subscribers))
			select {
			// Add new subscriber
			case subscriber := <-s.register:
				s.subscribers = append(s.subscribers, subscriber)

			// Remove closed subscriber
			case subscriber := <-s.unregister:
				i := s.getSubIndex(subscriber)
				if i != -1 {
					s.removeSubscriber(i)
				}

				// Publish the event to subscribers
			case event := <-s.event:
				fmt.Println("got event")
				for i := range s.subscribers {
					fmt.Printf("publishing to subscriber %d\n", i)
					s.subscribers[i].Connection <- event
				}

			// Shutdown if server closes
			case <-s.quit:
				// remove connections
				for i := range s.subscribers {
					s.removeSubscriber(i)
				}
				return
			}
		}
	}(str)
}

func (str *Stream) removeSubscriber(i int) {
	close(str.subscribers[i].Connection)
	str.subscribers = append(str.subscribers[:i], str.subscribers[i+1:]...)
}

func (str *Stream) getSubIndex(sub *Subscriber) int {
	for i := range str.subscribers {
		if str.subscribers[i] == sub {
			return i
		}
	}
	return -1
}

func (str *Stream) close() {
	str.quit <- true
}
