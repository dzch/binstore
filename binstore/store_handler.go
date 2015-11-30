/*
    The MIT License (MIT)
    
    Copyright (c) 2015 zhouwench zhouwench@gmail.com
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
package binstore

import (
		"github.com/dzch/go-utils/logger"
		"github.com/tinylib/msgp/msgp"
		"net/http"
		"time"
		"sync"
		"bytes"
		"fmt"
		"errors"
	   )

type StoreHandler struct {
	bs *BinStore
	reqPool *sync.Pool
}

type StoreReq struct {
	reqBuffer *bytes.Buffer
	data []byte
	id uint64
}

func newStoreHandler(bs *BinStore) (*StoreHandler, error) {
    h := &StoreHandler {
        bs: bs,
	}
    err := h.init()
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *StoreHandler) init() error {
    err := h.initReqPool()
	if err != nil {
		return err
	}
	return nil
}

func (h *StoreHandler) initReqPool() error {
	h.reqPool = &sync.Pool {
        New: h.newStoreReq,
	}
	return nil
}

func (h *StoreHandler) newStoreReq() interface{} {
    sr := &StoreReq {
        reqBuffer: &bytes.Buffer{},
	}
	return sr
}

func (h *StoreHandler) getStoreReq() *StoreReq {
	return h.reqPool.Get().(*StoreReq)
}

func (h *StoreHandler) putStoreReq(sr *StoreReq) {
	sr.reqBuffer.Reset()
	if sr.reqBuffer.Len() > gReqPoolBufferMaxLen {
		sr.reqBuffer = &bytes.Buffer{}
	}
	h.reqPool.Put(sr)
}

func (h *StoreHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    startTime := time.Now()
	/* check query */
	if r.ContentLength <= 0 {
		logger.Warning("invalid query, need post data: %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
    // qv := r.URL.Query()
	sr := h.getStoreReq()
	defer h.putStoreReq(sr)
	nr, err := sr.reqBuffer.ReadFrom(r.Body)
	if int64(nr) != r.ContentLength || err != nil {
		logger.Warning("fail to read body: %s, %s", r.URL.String(), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = h.parseReq(sr)
	if err != nil {
		logger.Warning("invalid query, parseReq failed : %s, %s", r.URL.String(), err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = h.bs.store.addNewData(sr)
	if err != nil {
		logger.Warning("fail to write response: %s, %s", r.URL.String(), err.Error())
	    w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
    endTime := time.Now()
	costTimeUS := endTime.Sub(startTime)/time.Microsecond
	logger.Notice("success process store: %s, id=%d, cost_us=%d", r.URL.String(), sr.id, costTimeUS)
	return
}

func (h *StoreHandler) parseReq(sr *StoreReq) error {
	/* unpack */
    msgr := msgp.NewReader(sr.reqBuffer)
	reqi, err := msgr.ReadIntf()
	if err != nil {
        return errors.New(fmt.Sprintf("fail to decode data from storeReq: %s", err.Error()))
	}
	req, ok := reqi.(map[string]interface{})
	if !ok {
        return errors.New(fmt.Sprintf("invalid data from storeReq: data need be map"))
	}
	datai, ok := req["data"]
	if !ok {
        return errors.New(fmt.Sprintf("invalid data from storeReq: data not exist"))
	}
	sr.data = datai.([]byte)
	idi, ok := req["id"]
	if !ok {
        return errors.New(fmt.Sprintf("invalid data from storeReq: id not exist"))
	}
	sr.id, ok = idi.(uint64)
	if !ok {
		idt, ok := idi.(int64)
		if !ok || idt < 0 {
			return errors.New(fmt.Sprintf("invalid data from storeReq: id is not int64 or uint64, or < 0"))
		}
	    sr.id = uint64(idt)
	}
	return nil
}
