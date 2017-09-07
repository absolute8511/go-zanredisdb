package zanredisdb

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

func newDeadlineTransport(timeout time.Duration) *http.Transport {
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

// stores the result in the value pointed to by ret(must be a pointer)
func apiRequest(method string, endpoint string, body io.Reader, ret interface{}) (int, error) {
	httpclient := &http.Client{Transport: newDeadlineTransport(time.Second)}
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return -1, err
	}
	resp, err := httpclient.Do(req)
	if err != nil {
		return -1, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return -1, err
	}

	if resp.StatusCode != 200 {
		return resp.StatusCode, fmt.Errorf("got response %s %q", resp.Status, respBody)
	}

	if len(respBody) == 0 {
		respBody = []byte("{}")
	}
	return resp.StatusCode, json.Unmarshal(respBody, ret)
}
