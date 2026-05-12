package gosse

type Subscriber struct {
	Quit       chan *Subscriber
	Connection chan []byte
}
