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

// concurrent safe
type DeDup struct {
	config *Config
	dialSession *mgo.Session
}

type DeDupResponse struct {
	Key string "key"
}

func newDeDup(bs *BinStore) (*DeDup, error) {
    dd := &DeDup{
        config: bs.config,
	}
	err := dd.init()
	if err != nil {
		return nil, err
	}
	return dd, nil
}

func (dd *DeDup) init() error {
    err := dd.initSession()
	if err != nil {
		return err
	}
	return nil
}

func (dd *DeDup) initSession() error {
    servers := strings.Join(dd.config.ddServerList, ",")
    url := "mongodb://" + servers
    s, err := mgo.DialWithTimeout(url, dd.config.ddConnTimeout)
	if err != nil {
		return err
	}
	s.SetSyncTimeout(dd.config.ddOperationTimeout)
	s.SetSocketTimeout(dd.config.ddSocketTimeout)
	s.SetSafe(&mgo.Safe{W: 1})
	s.SetMode(mgo.Strong, false)
	s.SetPoolLimit(dd.config.ddMgoPoolSize)
	dd.dialSession = s
	return nil
}

func (dd *DeDup) checkDup(ad *AddData) error {
	var rsp DeDupResponse
    dds := dd.dialSession.Copy()
	defer dds.Close()
	c := dds.DB(dd.config.ddDbName).C(dd.config.ddCollName)
	err := c.Find(bson.M{"fnv1a": ad.fnv1a32, "md5a": ad.md5a, "md5b": ad.md5b}).Select(bson.M{"key": 1, "_id": 0}).One(&rsp)
	if err == mgo.ErrNotFound {
		ad.Key = ""
		return nil
	}
	if err != nil {
		return err
	}
	ad.Key = rsp.Key
	return nil
}

func (dd *DeDup) insertNew(ad *AddData) error {
    dds := dd.dialSession.Copy()
	defer dds.Close()
	c := dds.DB(dd.config.ddDbName).C(dd.config.ddCollName)
	return c.Insert(bson.M{"fnv1a": ad.fnv1a32, "md5a": ad.md5a, "md5b": ad.md5b, "key": ad.Key})
}
