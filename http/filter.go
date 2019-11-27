package http

import "net/http"

// ResponseValidationFunc validates a response
type ResponseValidationFunc func(resp *http.Response) bool

// ResponsesFilterFunc filters a set of responses
type ResponsesFilterFunc func(responses []*http.Response, filterFunc ResponseValidationFunc) []*http.Response
