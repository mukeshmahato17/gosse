package gosse

import (
	"fmt"
	"net/http"
)

func (s *Server) HTTPHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming Unsupported!", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	streamID := r.URL.Query().Get("stream")
	stream := s.getStream(streamID)
	if stream == nil && !s.AutoStream {
		http.Error(w, "Stream not found!", http.StatusInternalServerError)
		return
	} else if stream == nil && s.AutoStream {
		s.CreateStream(streamID)
	}

	sub := stream.addSubscriber()
	defer sub.close()

	// Extract the request context to monitor for client disconnection or server shutdown
	ctx := r.Context()

	// Push events to client safely
	for {
		select {
		case <-ctx.Done():
			// Client disconnected or server is shutting down. Exit cleanly!
			return

		case msg, open := <-sub.connection:
			if !open {
				// Subscriber channel was closed elsewhere
				return
			}

			// Write event payload
			_, err := fmt.Fprintf(w, "data: %s\n", msg)
			if err != nil {
				// If writing fails (client vanished), stop processing
				return
			}
			flusher.Flush()
		}
	}
}
