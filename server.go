package gosse

const (
	// DefaultBufferSize size of the queue that holds the streams messages.
	DefaultBufferSize = 1024
)

type Server struct {
	BufferSize       int
	streams          map[string]*Stream
	registerStream   chan StreamRegistration
	unregisterStream chan string
	quit             chan bool
}

type StreamRegistration struct {
	id     string
	stream *Stream
}

func NewServer() *Server {
	return &Server{
		BufferSize:       DefaultBufferSize,
		streams:          make(map[string]*Stream),
		registerStream:   make(chan StreamRegistration),
		unregisterStream: make(chan string),
		quit:             make(chan bool),
	}
}

func (s *Server) Run() {
	go func(s *Server) {
		for {
			select {
			case reg := <-s.registerStream:
				s.streams[reg.id] = reg.stream

			case id := <-s.unregisterStream:
				s.streams[id].close()
				delete(s.streams, id)

			case <-s.quit:
				for id := range s.streams {
					s.streams[id].quit <- true
					delete(s.streams, id)
				}
				return
			}
		}
	}(s)
}

func (s *Server) CreateStream(id string) *Stream {
	sr := StreamRegistration{
		id:     id,
		stream: NewStream(s.BufferSize),
	}
	s.registerStream <- sr

	return sr.stream
}

// RemoveStream will remove all the stream
func (s *Server) RemoveStream(id string) {
	s.unregisterStream <- id
}

// GetStream returns a Stream entity based on its id
func (s *Server) GetStream(id string) *Stream {
	return s.streams[id]
}
