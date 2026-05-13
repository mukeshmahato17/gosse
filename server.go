package gosse

import (
	"fmt"
	"net/http"
	"sync"
)

const (
	// DefaultBufferSize size of the queue that holds the streams messages.
	DefaultBufferSize = 1024
)

type Server struct {
	BufferSize    int
	streams       map[string]*Stream
	DefaultStream bool
	mu            sync.Mutex
}

// New will create a server and setup defaults
func New() *Server {
	return &Server{
		BufferSize:    DefaultBufferSize,
		streams:       make(map[string]*Stream),
		DefaultStream: false,
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
	sub := s.getStream(streamID).addSubscriber()
	defer sub.Close()

	if streamID == "" && !s.DefaultStream {
		http.Error(w, "stream not found", http.StatusInternalServerError)
		return
	}

	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		sub.Close()
	}()
	for {
		fmt.Fprintf(w, "data: %s\n\n", <-sub.Connection)
		flusher.Flush()
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

// Publish sends a message to every client in a streamID
// Publish sends an event to every subscribers of the stream
func (s *Server) Publish(id string, event []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.streams[id].event <- event
}

func (s *Server) getStream(id string) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.streams[id]
}
