package gosse

type Subscriber struct {
	quit       chan *Subscriber
	connection chan []byte
}

// Close will let the stream know that the client connection has terminated
func (sub *Subscriber) close() {
	sub.quit <- sub
}
