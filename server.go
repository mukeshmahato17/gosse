package gosse

import (
	"sync"
)

const (
	// DefaultBufferSize size of the queue that holds the streams messages.
	DefaultBufferSize = 1024
)

type Server struct {
	BufferSize int
	streams    map[string]*Stream
	mu         sync.Mutex
}

// New will create a server and setup defaults
func New() *Server {
	return &Server{
		BufferSize: DefaultBufferSize,
		streams:    make(map[string]*Stream),
	}
}

// Close shuts down the server and close all the streams and connections
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.streams {
		s.streams[id].quit <- true
		delete(s.streams, id)
	}
}

// CreateStream will create a new stream and register it
func (s *Server) CreateStream(id string) *Stream {
	str := newStream(s.BufferSize)
	str.run()

	s.mu.Lock()
	defer s.mu.Unlock()

	// register that stream
	s.streams[id] = str

	return str
}

// RemoveStream will remove all the stream
func (s *Server) RemoveStream(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.streams[id].close()
	delete(s.streams, id)
}

func (s *Server) StreamExists(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.streams[id] != nil {
		return true
	}

	return false
}

// Publish sends a message to every client in a streamID
// Publish sends an event to every subscribers of the stream
func (s *Server) Publish(id string, event []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.streams[id] != nil {
		s.streams[id].event <- event
	}
}

func (s *Server) getStream(id string) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.streams[id]
}
