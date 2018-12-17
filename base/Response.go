package base

import "net/http"

type Response struct {
	httpResp *http.Response
	depth    uint32
}

func NewResponse(response *http.Response, depth uint32) *Response {
	return &Response{response, depth}
}

func (resp *Response) HttpResp() *http.Response {
	return resp.httpResp
}

func (resp *Response) Depth() uint32 {
	return resp.depth
}

func (resp *Response) Valid() bool {
	return resp.httpResp != nil && resp.httpResp.Body != nil
}
