package kittenerror

import "fmt"

// Custom error type is needed to be able to distinguish different error types.
type Custom struct {
	Code  int32
	Resp  string
	Descr string
}

func NewCustom(code int32, resp string, descr string) *Custom {
	return &Custom{Code: code, Resp: resp, Descr: descr}
}

func (e *Custom) Error() string {
	return fmt.Sprintf(`Error %d: "%s" %s`, e.Code, e.Resp, e.Descr)
}

func (e *Custom) GetCode() int32 {
	return e.Code
}

func (e *Custom) GetResp() string {
	return e.Resp
}

func (e *Custom) GetDescr() string {
	return e.Descr
}