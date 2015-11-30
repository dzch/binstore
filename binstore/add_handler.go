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
		"net/http"
		"time"
	   )

type AddHandler struct {
	bs *BinStore
}

func newAddHandler(bs *BinStore) *AddHandler {
	return &AddHandler {bs: bs}
}

func (h *AddHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    startTime := time.Now()
	/* check query */
	if r.ContentLength <= 0 {
		logger.Warning("invalid query, need post data: %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
    // qv := r.URL.Query()
	bs := h.bs
	ad := bs.adp.fetch()
	defer bs.adp.put(ad)
	ad.buffer.Reset()
	nr, err := ad.buffer.ReadFrom(r.Body)
	if int64(nr) != r.ContentLength || err != nil {
		logger.Warning("fail to read body: %s, %s", r.URL.String(), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ad.checksum()
	ad.Key = "";
	err = bs.dd.checkDup(ad)
	id := uint64(0)
	if err != nil {
		logger.Warning("fail to dd.checkDup: %s, %s", r.URL.String(), err.Error())
		// TODO: continue ?
	}
	if len(ad.Key) > 0 {
		// duplication
		// done
	} else {
		// new data
		id, err = bs.km.getNewId()
		if err != nil {
			logger.Warning("fail to getNewId: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		p, o, err := bs.broker.addNewData(id, ad)
		if err != nil {
			logger.Warning("fail to addNewData: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		key, err := bs.km.generateKey(id, p, o) 
		if err != nil {
			logger.Warning("fail to generateKey: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ad.Key = key
        err = bs.dd.insertNew(ad)
		if err != nil {
			logger.Warning("fail to dd.insertNew: %s", err.Error())
			// only warning here
		}
	}
	// response ok
	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(ad.Key))
	if err != nil {
		logger.Warning("fail to write response: %s, %s", r.URL.String(), err.Error())
	    w.WriteHeader(http.StatusInternalServerError)
		return
	}
    endTime := time.Now() 
	costTimeUS := endTime.Sub(startTime)/time.Microsecond
	logger.Notice("success process add: %s, cost_us=%d, datalen=%d, id=%d, md5a=%d, md5b=%d, fnv1a32=%d, key=%s", r.URL.String(), costTimeUS, nr, id, ad.md5a, ad.md5b, ad.fnv1a32, ad.Key)
	return
}
