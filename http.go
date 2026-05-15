package gosse

import (
	"fmt"
	"net/http"
)

func (s *Server) HTTPHandler(w http.ResponseWriter, r *http.Request) {
	flusher, err := w.(http.Flusher)
	if !err {
		http.Error(w, "Stream Unsupported!", http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	streamID := r.URL.Query().Get("stream")
	stream := s.getStream(streamID)

	if stream == nil {
		http.Error(w, "Stream not found!", http.StatusInternalServerError)
		return
	}

	sub := stream.addSubscriber()
	defer sub.close()

	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		sub.close()
	}()

	// Push events to client
	for {
		fmt.Fprintf(w, "data: %s\n\n", <-sub.connection)
		flusher.Flush()
	}
}
