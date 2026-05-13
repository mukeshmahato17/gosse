package gosse

type Subscriber struct {
	quit       chan *Subscriber
	Connection chan []byte
}

// Close will let the stream know that the client connection has terminated
func (sub *Subscriber) Close() {
	sub.quit <- sub
}
