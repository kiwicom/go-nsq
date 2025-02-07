package nsq

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
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

type wrappedResp struct {
	Status     string      `json:"status_txt"`
	StatusCode int         `json:"status_code"`
	Data       interface{} `json:"data"`
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return "unknown"
	}
	return addr.String()
}

// stores the result in the value pointed to by ret(must be a pointer)
func apiRequestNegotiateV1(httpclient *http.Client, method string, endpoint string, headers http.Header, ret interface{}) error {
	req, err := http.NewRequest(method, endpoint, nil)
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header[k] = v
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	var addrMutex sync.Mutex
	var addrs []string

	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			text := fmt.Sprintf("%q -> %q",
				addrString(info.Conn.LocalAddr()),
				addrString(info.Conn.RemoteAddr()))
			addrMutex.Lock()
			addrs = append(addrs, text)
			addrMutex.Unlock()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		addrMutex.Lock()
		addrsString := strings.Join(addrs, ", ")
		addrMutex.Unlock()
		return fmt.Errorf("for (%s) got response %s %q", addrsString, resp.Status, respBody)
	}

	if len(respBody) == 0 {
		respBody = []byte("{}")
	}

	if resp.Header.Get("X-NSQ-Content-Type") == "nsq; version=1.0" {
		return json.Unmarshal(respBody, ret)
	}

	wResp := &wrappedResp{
		Data: ret,
	}

	if err = json.Unmarshal(respBody, wResp); err != nil {
		return err
	}

	// wResp.StatusCode here is equal to resp.StatusCode, so ignore it
	return nil
}
