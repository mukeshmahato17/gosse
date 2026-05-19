package gosse

import (
	"bufio"
	"bytes"
	"context"
	"net/http"
)

var (
	headerID    = []byte("id:")
	headerData  = []byte("data:")
	headerEvent = []byte("event:")
	headerError = []byte("error:")
)

type Client struct {
	URL        string
	Connection *http.Client
	Header     map[string]string
}

func NewClient(url string) *Client {
	return &Client{
		URL:        url,
		Connection: &http.Client{},
	}
}

func (c *Client) Subscribe(ctx context.Context, stream string, handler func(msg *Event)) error {
	resp, err := c.request(ctx, stream)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	for {
		// if the context is canceled outside, exit te loop immidiately
		if err := ctx.Err(); err != nil {
			return err
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}

		msg := processEvent(line)
		if msg != nil {
			handler(msg)
		}
	}
}

func (c *Client) SubscribeChan(ctx context.Context, stream string, ch chan *Event) error {
	resp, err := c.request(ctx, stream)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			close(ch)
			return err
		}
		msg := processEvent(line)
		if msg != nil {
			ch <- msg
		}
	}
}

func (c *Client) request(ctx context.Context, stream string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.URL, nil)
	if err != nil {
		return nil, err
	}

	// Setup request, specify stream to connect to
	query := req.URL.Query()
	query.Add("stream", stream)
	req.URL.RawQuery = query.Encode()
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Connection", "keep-alive")

	for k, v := range c.Header {
		req.Header.Set(k, v)
	}

	return c.Connection.Do(req)
}

func processEvent(msg []byte) *Event {
	var e Event

	switch h := msg; {
	case bytes.Contains(h, headerID):
		e.ID = trimHeader(len(headerID), msg)
	case bytes.Contains(h, headerData):
		e.Data = trimHeader(len(headerData), msg)
	case bytes.Contains(h, headerEvent):
		e.Event = trimHeader(len(headerEvent), msg)
	case bytes.Contains(h, headerError):
		e.Error = trimHeader(len(headerError), msg)
	default:
		return nil
	}

	return &e
}

func trimHeader(size int, data []byte) []byte {
	data = data[size:]
	// Remove optional leading whitespace
	if data[0] == 32 {
		data = data[1:]
	}

	// Remove Trailing new line
	if data[len(data)-1] == 10 {
		data = data[:len(data)-1]
	}

	return data
}
