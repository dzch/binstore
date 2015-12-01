/*
    The MIT License (MIT)
    
	Copyright (c) 2015 myhug.cn and zhouwench (zhouwench@gmail.com)
    
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
		"errors"
		"net/http"
		"time"
	   )

type GetHandler struct {
	bs *BinStore
}

func newGetHandler(bs *BinStore) *GetHandler {
    gh := &GetHandler {
        bs: bs,
	}
	return gh
}

func (h *GetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    startTime := time.Now()
    qv := r.URL.Query()
	id, partition, offset, err := h.bs.km.parseKey(qv.Get("key"))
	if err != nil {
		logger.Warning("fail to parseKey: %s, %s", r.URL.String(), err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	val, err := h.getData(id, partition, offset)
	if err != nil {
		logger.Warning("fail to getData: %s, %s", r.URL.String(), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(val)
	if err != nil {
		logger.Warning("fail to write response: %s, %s", r.URL.String(), err.Error())
	    w.WriteHeader(http.StatusInternalServerError)
		return
	}
    endTime := time.Now()
	costTimeUS := endTime.Sub(startTime)/time.Microsecond
	logger.Notice("success process get: %s, cost_us=%d, datalen=%d", r.URL.String(), costTimeUS, len(val))
	return
}

func (h *GetHandler) getData(id uint64, partition int32, offset int64) ([]byte, error) {
	inBroker, err := h.bs.zk.dataInBroker(partition, offset)
	if err != nil {
		return nil, err
	}
	if inBroker {
		return h.bs.broker.getData(partition, offset)
	} else {
		return h.bs.store.getData(id)
	}
	return nil, errors.New("fatal Error for getData !")
}
