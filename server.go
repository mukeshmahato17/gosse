package gosse

import (
	"fmt"
	"net/http"
)

const (
	// DefaultBufferSize size of the queue that holds the streams messages.
	DefaultBufferSize = 1024
)

type Server struct {
	BufferSize       int
	streams          map[string]*Stream
	registerStream   chan StreamRegistration
	deregisterStream chan string
	quit             chan bool
}

func NewServer() *Server {
	return &Server{
		BufferSize:       DefaultBufferSize,
		streams:          make(map[string]*Stream),
		registerStream:   make(chan StreamRegistration),
		deregisterStream: make(chan string),
		quit:             make(chan bool),
	}
}

// Start server's internal messaging
func (s *Server) Start() {
	go func(s *Server) {
		for {
			select {
			// Add new stream
			case reg := <-s.registerStream:
				s.streams[reg.id] = reg.stream

			// remove old stream
			case id := <-s.deregisterStream:
				s.streams[id].close()
				delete(s.streams, id)

			// close all streams
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

// HTTPHandler serves a new connection with events for a given stream
func (s *Server) HTTPHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Get the streamID from
	streamID := r.URL.Query().Get("streamID")

	stream := s.GetStream(streamID)
	if stream == nil {
		http.Error(w, "stream not found", http.StatusInternalServerError)
		return
	}

	sub := stream.NewSubscriber()
	defer sub.Close()

	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		sub.Close()
	}()
	for {
		// Write to ResponseWriter
		// Server Sent Event compatible
		fmt.Fprintf(w, "data: %s\n\n", <-sub.Connection)
		// Flush the data immediatly instead of buffering it for later.
		flusher.Flush()
	}
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
	s.deregisterStream <- id
}

// GetStream returns a Stream entity based on its id
func (s *Server) GetStream(id string) *Stream {
	// Hashmap is unsafe, might cause race condition if register/deredister
	// run at same time
	return s.streams[id]
}
