package gosse

type Subscriber struct {
	Quit       chan *Subscriber
	Connection chan []byte
}

// Close will let the stream know that the client connection has terminated
func (sub *Subscriber) Close() {
	sub.Quit <- sub
}
