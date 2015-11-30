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
		zookeeper "github.com/samuel/go-zookeeper/zk"
		"fmt"
		"sync"
		"strconv"
		"errors"
		"time"
	   )


var (
		gConsumerName = "binstore"
	)

type ZK struct {
	config *Config
	offsetPPath string
	offsets []int64
	offsetsTmp []int64
	offsetsLock *sync.RWMutex
}

func newZK(bs *BinStore) (*ZK, error) {
    zk := &ZK {
        config: bs.config,
		offsetPPath: fmt.Sprintf("%s/consumers/%s/offsets/%s", bs.config.zkChroot, gConsumerName, gBrokerTopic),
		offsetsLock: &sync.RWMutex{},
	}
	err := zk.init()
	if err != nil {
		return nil, err
	}
    return zk, nil
}

func (zk *ZK) init() error {
    err := zk.initOffsets()
	if err != nil {
		return err
	}
	return nil
}

func (zk *ZK) initOffsets() error {
	zk.offsets = make([]int64, gPartitionMaxValue)
	zk.offsetsTmp = make([]int64, gPartitionMaxValue)
    return zk.updateOffsets()
}

func (zk *ZK) updateOffsets() error {
	conn, _, err := zookeeper.Connect(zk.config.zkHosts, zk.config.zkSessionTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
    children, _, err := conn.Children(zk.offsetPPath)
	if err == zookeeper.ErrNoNode {
		return nil
	}
	if err != nil {
		return err
	}
	if len(children) == 0 {
		return nil
	}
	for _, node := range children {
        val, _, err := conn.Get(fmt.Sprintf("%s/%s", zk.offsetPPath, node))
		if err != nil {
			return err
		}
		offset, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return err
		}
        partition, err := strconv.Atoi(node)
		if err != nil {
			return err
		}
		zk.offsetsTmp[partition] = offset
	}
	zk.offsetsLock.Lock()
	defer zk.offsetsLock.Unlock()
	copy(zk.offsets, zk.offsetsTmp)
	return nil
}

func (zk *ZK) run() {
	for {
        err := zk.updateOffsets()
		if err != nil {
			logger.Warning("fail to zk.updateOffsets: %s", err.Error())
			time.Sleep(zk.config.zkFailRetryInterval)
		}
		time.Sleep(zk.config.zkUpdateInterval)
	}
}

func (zk *ZK) dataInBroker(partition int32, offset int64) (bool, error) {
	if partition > gPartitionMaxValue {
		return false, errors.New("partition is toooo big")
	}
	zk.offsetsLock.RLock()
	defer zk.offsetsLock.RUnlock()
	// consumer offset is next need to be commit, so need =
	if offset >= zk.offsets[partition] {
		return true, nil
	}
	return false, nil
}
