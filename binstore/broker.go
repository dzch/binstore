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
		"github.com/Shopify/sarama"
		"github.com/tinylib/msgp/msgp"
		"errors"
		"fmt"
		"math/rand"
		"sort"
		"bytes"
	   )

var (
		gBrokerTopic = "binstore"
		gBrokerMethod = "binstore"
	)

type Broker struct {
	config *Config
	ap sarama.AsyncProducer
	brokerConfig *sarama.Config
	bc sarama.Client
	writeDisabledPartitions []int
	nWriteDisabledPartitions int
	produceChan chan *sarama.ProducerMessage
}

func newBroker(bs *BinStore) (*Broker, error) {
    b := &Broker {
        config: bs.config,
		writeDisabledPartitions: bs.config.brokerWDisabledPartitions,
		nWriteDisabledPartitions: len(bs.config.brokerWDisabledPartitions),
	}
	err := b.init()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) init() error {
    err := b.initBrokerConfig()
	if err != nil {
		return err
	}
    err = b.initAP()
	if err != nil {
		return err
	}
    err = b.initBC()
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) initBrokerConfig() error {
    pconfig := sarama.NewConfig()
	pconfig.Net.MaxOpenRequests = 10240
	pconfig.Net.DialTimeout = b.config.brokerConnTimeout
	pconfig.Net.ReadTimeout = b.config.brokerReadTimeout
	pconfig.Net.WriteTimeout = b.config.brokerWriteTimeout
	pconfig.Metadata.RefreshFrequency = b.config.brokerMetadataRefreshInterval
	pconfig.Producer.MaxMessageBytes = b.config.brokerMaxMessageSize
	pconfig.Producer.RequiredAcks = sarama.WaitForAll
	pconfig.Producer.Return.Successes = true
	pconfig.Producer.Return.Errors = true
	pconfig.Producer.Partitioner = sarama.NewManualPartitioner
	b.brokerConfig = pconfig
	return nil
}

func (b *Broker) initAP() error {
	var err error
	b.ap, err = sarama.NewAsyncProducer(b.config.brokerServerList, b.brokerConfig)
	if err != nil {
		return err
	}
	b.produceChan = make(chan *sarama.ProducerMessage, 64)
	return nil
}

func (b *Broker) initBC() error {
	var err error
	b.bc, err = sarama.NewClient(b.config.brokerServerList, b.brokerConfig)
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) run() {
	for {
		select {
			case pm := <-b.produceChan:
				b.produce(pm)
			case perr := <-b.ap.Errors():
				b.processProduceError(perr)
			case pm := <-b.ap.Successes():
				b.processProduceSuccess(pm)
		}
	}
}

func (b *Broker) produce(pm *sarama.ProducerMessage) {
	for {
		select {
			case b.ap.Input() <- pm:
				return
			case perr := <-b.ap.Errors():
				b.processProduceError(perr)
			case pm := <-b.ap.Successes():
				b.processProduceSuccess(pm)
		}
	}
}

func (b *Broker) processProduceError(perr *sarama.ProducerError) {
    pm := perr.Msg
	ad := pm.Metadata.(*AddData)
	ad.brokerErr = perr.Err
	ad.pmsg = pm
	ad.brokerDoneChan <- 1
}

func (b *Broker) processProduceSuccess(pm *sarama.ProducerMessage) {
    ad := pm.Metadata.(*AddData)
	ad.brokerErr = nil
	ad.pmsg = pm
	ad.brokerDoneChan <- 1
}

func (b *Broker) addNewData(id uint64, ad *AddData) (int32, int64, error) {
    data := map[string]interface{} {
        "id": id,
		"data": ad.buffer.Bytes(),
		"method": gBrokerMethod,
	}
	wr := ad.msgpWriter
	err := wr.WriteIntf(data)
	if err != nil {
		return 0, 0, errors.New(fmt.Sprintf("fail to msgp.WriteIntf: %s", err.Error()))
	}
	wr.Flush()
	ad.pmsg.Value = sarama.ByteEncoder(ad.msgpBuffer.Bytes())
	ad.pmsg.Metadata = ad
	ad.pmsg.Partition, err = b.getOneWritablePartition()
	if err != nil {
		return 0, 0, errors.New(fmt.Sprintf("fail to get one writable partition: %s", err.Error()))
	}
	b.produceChan <-ad.pmsg
	<-ad.brokerDoneChan
	if ad.brokerErr != nil {
		return 0, 0, errors.New(fmt.Sprintf("fail to produce: %s", ad.brokerErr.Error()))
	}
	return ad.pmsg.Partition, ad.pmsg.Offset, nil
}

func (b *Broker) getOneWritablePartition() (int32, error) {
	wp, err := b.bc.WritablePartitions(gBrokerTopic)
	if err != nil {
		return 0, err
	}
    wplen := len(wp)
	if wplen == 0 {
		return 0, errors.New("no writable partitions in broker")
	}
	i,j := rand.Int()%wplen, 0
	for ; j < wplen; j++ {
		if b.isPartitionBlocked(int(wp[i%wplen])) {
			i ++
			continue
		}
		break
	}
	if j == wplen {
		return 0, errors.New("no writable partitions in broker")
	}
	return wp[i], nil
}

func (b *Broker) isPartitionBlocked(partition int) bool {
	if b.nWriteDisabledPartitions == 0 {
		return false
	}
    idx := sort.SearchInts(b.writeDisabledPartitions, partition)
	if b.writeDisabledPartitions[idx] == partition {
		return true
	}
	return false
}

func (b *Broker) getData(partition int32, offset int64) ([]byte, error) {
	// TODO: using pool ?
	bb, err := b.bc.Leader(gBrokerTopic, partition)
	if err != nil {
		return nil, err
	}
	bbb := sarama.NewBroker(bb.Addr())
	defer bbb.Close()
	err = bbb.Open(b.brokerConfig)
	if err != nil {
		return nil, err
	}
    freq := &sarama.FetchRequest {}
	freq.AddBlock(gBrokerTopic, partition, offset, int32(b.config.brokerMaxMessageSize))
	fres, err := bbb.Fetch(freq)
	if err != nil {
		return nil, err
	}
    fresb := fres.GetBlock(gBrokerTopic, partition)
	msgs := fresb.MsgSet.Messages
	if len(msgs) == 0 {
		return nil, errors.New("no msg found in broker")
	}
	/* unpack */
	buf := bytes.NewReader(msgs[0].Msg.Value)
	buf.Seek(0, 0)
	msgr := msgp.NewReader(buf)
	resi, err := msgr.ReadIntf()
	if err != nil {
        return nil, errors.New(fmt.Sprintf("fail to decode data from broker: %s", err.Error()))
	}
	res, ok := resi.(map[string]interface{})
	if !ok {
        return nil, errors.New(fmt.Sprintf("invalid data from broker: data need be map"))
	}
	datai, ok := res["data"]
	if !ok {
        return nil, errors.New(fmt.Sprintf("invalid data from broker: data not exist"))
	}
	return datai.([]byte), nil
}
