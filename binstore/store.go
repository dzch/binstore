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
		"gopkg.in/mgo.v2"
		"gopkg.in/mgo.v2/bson"
		"strings"
	   )

type Store struct {
	config *Config
	dialSession *mgo.Session
}

type QueryResponse struct {
	Data []byte "data"
}

func newStore(bs *BinStore) (*Store, error) {
    store := &Store {
        config: bs.config,
	}
	err := store.init()
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (store *Store) init() error {
    err := store.initDialSession()
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) initDialSession() error {
    servers := strings.Join(store.config.storeServerList, ",")
    url := "mongodb://" + servers
    s, err := mgo.DialWithTimeout(url, store.config.storeConnTimeout)
	if err != nil {
		return err
	}
	s.SetSyncTimeout(store.config.storeOperationTimeout)
	s.SetSocketTimeout(store.config.storeSocketTimeout)
	s.SetSafe(&mgo.Safe{W: store.config.storeWriteConcern})
	s.SetMode(mgo.Eventual, false)
	s.SetPoolLimit(store.config.storeMgoPoolSize)
	store.dialSession = s
	return nil
}

func (store *Store) addNewData(sr *StoreReq) error {
	// can do many times repeateadly for one req
    s := store.dialSession.Copy()
	defer s.Close()
	c := s.DB(store.config.storeDbName).C(store.config.storeCollName)
	_, err := c.Upsert(bson.M{"id": sr.id}, bson.M{"id": sr.id, "data": sr.data})
	return err
}

func (store *Store) getData(id uint64) ([]byte, error) {
    s := store.dialSession.Copy()
	defer s.Close()
	res := &QueryResponse{}
	c := s.DB(store.config.storeDbName).C(store.config.storeCollName)
	err := c.Find(bson.M{"id": id}).One(res)
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}
