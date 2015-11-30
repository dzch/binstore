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
		"github.com/Shopify/sarama"
		"net/http"
		"fmt"
		"errors"
	   )

var (
		gReqPoolBufferMaxLen = 1*1024*1024
	)

type BinStore struct {
	confFile string
	config *Config
	fatalErrorChan chan error
	server *http.Server
	dd *DeDup
	adp *AddDataPool
	km *KeyManager
	broker *Broker
	zk *ZK
	store *Store
}

func NewBinStore(confFile string) (*BinStore, error) {
    bs := &BinStore {
        confFile: confFile,
	}
    err := bs.init()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (bs *BinStore) Run() {
	go bs.broker.run()
	go bs.zk.run()
	go bs.runHttpServer()
	err := <-bs.fatalErrorChan
	logger.Fatal("Fail: %s", err.Error())
	return
}

func (bs *BinStore) init() error {
    err := bs.initConfig()
	if err != nil {
		return errors.New(fmt.Sprintf("config: %s", err.Error()))
	}
	err = bs.initLogger()
	if err != nil {
		return errors.New(fmt.Sprintf("logger: %s", err.Error()))
	}
	err = bs.initChans()
	if err != nil {
		return err
	}
	err = bs.initHttpServer()
	if err != nil {
		return errors.New(fmt.Sprintf("http_server: %s", err.Error()))
	}
	err = bs.initDeDup()
	if err != nil {
		return errors.New(fmt.Sprintf("dedup: %s", err.Error()))
	}
	err = bs.initADP()
	if err != nil {
		return err
	}
	err = bs.initKeyManager()
	if err != nil {
		return errors.New(fmt.Sprintf("km: %s", err.Error()))
	}
	err = bs.initBroker()
	if err != nil {
		return errors.New(fmt.Sprintf("broker: %s", err.Error()))
	}
	err = bs.initZK()
	if err != nil {
		return errors.New(fmt.Sprintf("zk: %s", err.Error()))
	}
	err = bs.initStore()
	if err != nil {
		return errors.New(fmt.Sprintf("store: %s", err.Error()))
	}
	return nil
}

func (bs *BinStore) initConfig() error {
	var err error
	bs.config, err = newConfig(bs.confFile)
	return err
}

func (bs *BinStore) initLogger() error {
	sarama.Logger = newSaramaLogger()
    err := logger.Init(bs.config.logDir, "binstore", logger.LogLevel(bs.config.logLevel))
	return err
}

func (bs *BinStore) initChans() error {
	bs.fatalErrorChan = make(chan error, 1)
	return nil
}

func (bs *BinStore) initHttpServer() error {
    mux := http.NewServeMux()
	mux.Handle("/add", newAddHandler(bs))
	mux.Handle("/get", newGetHandler(bs))
	h, err := newStoreHandler(bs)
	if err != nil {
		return err
	}
	mux.Handle("/store", h)
    bs.server = &http.Server {
        Addr: fmt.Sprintf(":%d", bs.config.httpServerListenPort),
		Handler: mux,
		ReadTimeout: bs.config.httpServerReadTimeout,
		WriteTimeout: bs.config.httpServerWriteTimeout,
	}
	return nil
}

func (bs *BinStore) initDeDup() error {
	var err error
	bs.dd, err = newDeDup(bs)
	return err
}

func (bs *BinStore) initADP() error {
	bs.adp = newAddDataPool()
	return nil
}

func (bs *BinStore) initKeyManager() error {
	var err error
	bs.km, err = newKeyManager(bs)
	return err
}

func (bs *BinStore) initBroker() error {
	var err error
	bs.broker, err = newBroker(bs)
	return err
}

func (bs *BinStore) initZK() error {
	var err error
	bs.zk, err = newZK(bs)
	return err
}

func (bs *BinStore) initStore() error {
	var err error
	bs.store, err = newStore(bs)
	return err
}

func (bs *BinStore) runHttpServer() {
    err := bs.server.ListenAndServe()
	if err != nil {
		logger.Fatal("fail to start http server: %s", err.Error())
		bs.fatalErrorChan <- err
		return
	}
	err = errors.New("http server done")
	bs.fatalErrorChan <- err
}

