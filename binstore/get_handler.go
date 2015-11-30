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
