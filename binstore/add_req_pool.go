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
		"github.com/Shopify/sarama"
		"github.com/tinylib/msgp/msgp"
		"sync"
		"bytes"
		"hash"
		"hash/fnv"
		"crypto/md5"
		"encoding/binary"
	   )

var (
		gAddDataBufferMaxLen = 2*1024*1024
	)

type AddData struct {
	fnv1a hash.Hash32
	md5 hash.Hash
	// data
	buffer *bytes.Buffer
	// check sum
	fnv1a32 uint32
	md5a uint64
	md5b uint64
	// key
	Key string "key"
	// broker
	pmsg *sarama.ProducerMessage
	brokerErr error
	brokerDoneChan chan int
	msgpBuffer *bytes.Buffer
	msgpWriter *msgp.Writer
}

func (ad *AddData) checksum() {
	ad.fnv1a.Reset()
    ad.fnv1a.Write(ad.buffer.Bytes())
	ad.fnv1a32 = ad.fnv1a.Sum32()
	ad.md5.Reset()
	ad.md5.Write(ad.buffer.Bytes())
	md5 := ad.md5.Sum(nil)
	ad.md5a = binary.LittleEndian.Uint64(md5[0:8])
	ad.md5b = binary.LittleEndian.Uint64(md5[8:16])
}

type AddDataPool struct {
    pool *sync.Pool
}

func newAddDataPool() *AddDataPool {
    adp := &AddDataPool {
        pool: &sync.Pool{},
		 }
	adp.pool.New = newAddData
	return adp
}

func (adp *AddDataPool) fetch() *AddData {
    ad := adp.pool.Get().(*AddData)
	return ad
}

func (adp *AddDataPool) put(ad *AddData) {
	ad.buffer.Reset()
	ad.msgpBuffer.Reset()
	if ad.buffer.Len() > gAddDataBufferMaxLen {
		ad.buffer = &bytes.Buffer{}
	}
	if ad.msgpBuffer.Len() > gAddDataBufferMaxLen {
		ad.msgpBuffer = &bytes.Buffer{}
        ad.msgpWriter = msgp.NewWriter(ad.msgpBuffer)
	}
	adp.pool.Put(ad)
}

func newAddData() interface{} {
    ad := &AddData {
		buffer: &bytes.Buffer{},
		msgpBuffer: &bytes.Buffer{},
	    fnv1a: fnv.New32a(),
		md5: md5.New(),
		Key: "",
		pmsg: &sarama.ProducerMessage{Topic: gBrokerTopic},
		brokerDoneChan: make(chan int, 1),
    }
    ad.msgpWriter = msgp.NewWriter(ad.msgpBuffer)
	return ad
}

