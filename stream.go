package gosse

type Stream struct {
	subscribers []*Subscriber
	register    chan *Subscriber
	deregister  chan *Subscriber
	event       chan []byte
	quit        chan bool
}

type StreamRegistration struct {
	id     string
	stream *Stream
}

func newStream(bufSize int) *Stream {
	return &Stream{
		subscribers: make([]*Subscriber, 0),
		register:    make(chan *Subscriber),
		deregister:  make(chan *Subscriber),
		event:       make(chan []byte, bufSize),
		quit:        make(chan bool),
	}
}

func (str *Stream) run() {
	go func(s *Stream) {
		for {
			select {
			// Add new subscriber safely inside the loop
			case subscriber := <-s.register:
				s.subscribers = append(s.subscribers, subscriber)

			// Remove closed subscriber safely inside the loop
			case subscriber := <-s.deregister:
				i := s.getSubIndex(subscriber)
				if i != -1 {
					s.executeRemoval(i)
				}

			// Publish events to all active subscribers
			case event := <-s.event:
				for i := range s.subscribers {
					s.subscribers[i].connection <- event
				}

			// Shutdown server and purge everything cleanly
			case <-s.quit:
				// Loop BACKWARDS to safely delete elements without breaking index paths
				for i := len(s.subscribers) - 1; i >= 0; i-- {
					s.executeRemoval(i)
				}
				return
			}
		}
	}(str)
}

func (str *Stream) addSubscriber() *Subscriber {
	sub := &Subscriber{
		quit:       str.deregister,
		connection: make(chan []byte, 64),
	}

	str.register <- sub
	return sub
}

// Public API triggers a channel event so the background loop handles the delete
func (str *Stream) removeSubscriber(sub *Subscriber) {
	str.deregister <- sub
}

// Internal helper ONLY called inside the select-loop to mutate the slice safely
func (s *Stream) executeRemoval(i int) {
	close(s.subscribers[i].connection)
	s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
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
