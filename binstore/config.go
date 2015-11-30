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
		"github.com/go-yaml/yaml"
		"time"
		"io/ioutil"
		"errors"
		"fmt"
		"strings"
		"strconv"
		"sort"
	   )

type Config struct {
	confFile string
	confParsed map[interface{}]interface{}
	// kproxy common
	logDir string
	logLevel int
	// http server
	httpServerListenPort uint16
	httpServerReadTimeout time.Duration
	httpServerWriteTimeout time.Duration
	// dedup
	ddServerList []string
	ddConnTimeout time.Duration
	ddSocketTimeout time.Duration
	ddOperationTimeout time.Duration
	ddMgoPoolSize int
	ddDbName string
	ddCollName string
	// key manager
	kmOddRedisAddr string
	kmEvenRedisAddr string
	kmFCryptKey string
	kmRedisConnTimeout time.Duration
	kmRedisReadTimeout time.Duration
	kmRedisWriteTimeout time.Duration
	kmRedisMinConnEach int
	kmRedisMaxConnEach int
	kmRedisPoolIdleTimeout time.Duration
	kmKeyTag string
	// broker
	brokerServerList []string
	brokerWDisabledPartitions []int
	brokerConnTimeout time.Duration
	brokerReadTimeout time.Duration
	brokerWriteTimeout time.Duration
	brokerMaxMessageSize int
	brokerMetadataRefreshInterval time.Duration
	// store
	storeServerList []string
	storeConnTimeout time.Duration
	storeSocketTimeout time.Duration
	storeOperationTimeout time.Duration
	storeMgoPoolSize int
	storeDbName string
	storeCollName string
	storeWriteConcern int
	// zk
	zkHosts []string
	zkSessionTimeout time.Duration
	zkChroot string
	zkUpdateInterval time.Duration
	zkFailRetryInterval time.Duration
}

func newConfig(confFile string) (*Config, error) {
    c := &Config {
		confFile: confFile,
	   }
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) init() error {
    err := c.parseConfFile()
	if err != nil {
		return err
	}
	err = c.initCommonConfig()
	if err != nil {
		return err
	}
	err = c.initHttpServerConfig()
	if err != nil {
		return err
	}
	err = c.initDeDupConfig()
	if err != nil {
		return err
	}
	err = c.initBrokerConfig()
	if err != nil {
		return err
	}
	err = c.initKeyManager()
	if err != nil {
		return err
	}
	err = c.initStoreConfig()
	if err != nil {
		return err
	}
	err = c.initZK()
	if err != nil {
		return err
	}
	c.dump()
	return nil
}

func (c *Config) parseConfFile() error {
    content, err := ioutil.ReadFile(c.confFile)
	if err != nil {
		return err
	}
    c.confParsed = make(map[interface{}]interface{})
	err = yaml.Unmarshal(content, &(c.confParsed))
	if err != nil {
		return err
	}
	return nil
}

func (c *Config) initCommonConfig() error {
	mi, ok := c.confParsed["common"]
	if !ok {
		return errors.New("common not found in conf file")
	}
    m, ok := mi.(map[interface{}]interface{})
	if !ok {
		return errors.New("common config is not map")
	}
    logDir, ok := m["log_dir"]
	if !ok {
		return errors.New("log_dir not found in conf file")
    }
	c.logDir = logDir.(string)
	logLevel, ok := m["log_level"]
	if !ok {
		return errors.New("log_level not found in conf file")
	}
	c.logLevel = logLevel.(int)
	return nil
}

func (c *Config) initHttpServerConfig() error {
	mi, ok := c.confParsed["http_server"]
	if !ok {
		return errors.New("http_server not found in conf file")
	}
    m, ok := mi.(map[interface{}]interface{})
	if !ok {
		return errors.New("http_server config is not map")
	}
	port, ok := m["port"]
	if !ok {
		return errors.New("port not found in conf file")
	}
	c.httpServerListenPort = uint16(port.(int))
	rtimeo, ok := m["read_timeout_ms"]
	if !ok {
		return errors.New("read_timeout_ms not found in conf file")
	}
	c.httpServerReadTimeout = time.Duration(rtimeo.(int))*time.Millisecond
	wtimeo, ok := m["write_timeout_ms"]
	if !ok {
		return errors.New("write_timeout_ms not found in conf file")
	}
	c.httpServerWriteTimeout = time.Duration(wtimeo.(int))*time.Millisecond
	return nil
}

func (c *Config) initDeDupConfig() error {
	mi, ok := c.confParsed["dedup"]
	if !ok {
		return errors.New("dedup not found in conf file")
	}
    m, ok := mi.(map[interface{}]interface{})
	if !ok {
		return errors.New("dedup config is not map")
	}
	deduphosts, ok := m["dedup_hosts"]
	if !ok {
		return errors.New("dedup_hosts not found in conf file")
	}
    dedupHosts := deduphosts.([]interface{})
	if len(dedupHosts) <= 0 {
		return errors.New("num of dedupHosts is zero")
	}
	for _, deduphost := range dedupHosts {
		c.ddServerList = append(c.ddServerList, deduphost.(string))
	}
	timeo, ok := m["conn_timeout_ms"]
	if !ok {
		return errors.New("dedup conn_timeout_ms not found in conf file")
	}
	c.ddConnTimeout = time.Duration(timeo.(int))*time.Millisecond
	timeo, ok = m["socket_timeout_ms"]
	if !ok {
		return errors.New("dedup socket_timeout_ms not found in conf file")
	}
	c.ddSocketTimeout = time.Duration(timeo.(int))*time.Millisecond
	timeo, ok = m["operation_timeout_ms"]
	if !ok {
		return errors.New("dedup operation_timeout_ms not found in conf file")
	}
	c.ddOperationTimeout = time.Duration(timeo.(int))*time.Millisecond
	poolSize, ok := m["mgo_pool_size"]
	if !ok {
		return errors.New("dedup mgo_pool_size not found in conf file")
	}
	c.ddMgoPoolSize = poolSize.(int)
	dbName, ok := m["database_name"]
	if !ok {
		return errors.New("dedup database_name not found in conf file")
	}
	c.ddDbName = dbName.(string)
	collName, ok := m["collection_name"]
	if !ok {
		return errors.New("dedup collection_name not found in conf file")
	}
	c.ddCollName = collName.(string)
	return nil
}

func (c *Config) initKeyManager() error {
	mi, ok := c.confParsed["km"]
	if !ok {
		return errors.New("km not found in conf file")
	}
    m, ok := mi.(map[interface{}]interface{})
	if !ok {
		return errors.New("km config is not map")
	}
	fckey, ok := m["fcrypt_key"]
	if !ok {
		return errors.New("km fcrypt_key not found in conf file")
	}
	c.kmFCryptKey = fckey.(string)
	keyt, ok := m["key_tag"]
	if !ok {
		return errors.New("km key_tag not found in conf file")
	}
	c.kmKeyTag = keyt.(string)
	oaddr, ok := m["odd_id_redis_addr"]
	if !ok {
		return errors.New("km odd_id_redis_addr not found in conf file")
	}
	c.kmOddRedisAddr = oaddr.(string)
	eaddr, ok := m["even_id_redis_addr"]
	if !ok {
		return errors.New("km even_id_redis_addr not found in conf file")
	}
	c.kmEvenRedisAddr = eaddr.(string)
	ctimeo, ok := m["conn_timeout_ms"]
	if !ok {
		return errors.New("km conn_timeout_ms not found in conf file")
	}
	c.kmRedisConnTimeout = time.Duration(ctimeo.(int))*time.Millisecond
	rtimeo, ok := m["read_timeout_ms"]
	if !ok {
		return errors.New("km read_timeout_ms not found in conf file")
	}
	c.kmRedisReadTimeout = time.Duration(rtimeo.(int))*time.Millisecond
	wtimeo, ok := m["write_timeout_ms"]
	if !ok {
		return errors.New("km write_timeout_ms not found in conf file")
	}
	c.kmRedisWriteTimeout = time.Duration(wtimeo.(int))*time.Millisecond
	itimeo, ok := m["idle_timeout_ms"]
	if !ok {
		return errors.New("km idle_timeout_ms not found in conf file")
	}
	c.kmRedisPoolIdleTimeout = time.Duration(itimeo.(int))*time.Millisecond
	num, ok := m["min_conn_each"]
	if !ok {
		return errors.New("km min_conn_each not found in conf file")
	}
	c.kmRedisMinConnEach = num.(int)
	num, ok = m["max_conn_each"]
	if !ok {
		return errors.New("km max_conn_each not found in conf file")
	}
	c.kmRedisMaxConnEach = num.(int)
    return nil
}

func (c *Config) initBrokerConfig() error {
	mi, ok := c.confParsed["broker"]
	if !ok {
		return errors.New("broker not found in conf file")
	}
    m, ok := mi.(map[interface{}]interface{})
	if !ok {
		return errors.New("broker config is not map")
	}
	brokerhosts, ok := m["broker_hosts"]
	if !ok {
		return errors.New("broker: broker_hosts not found in conf file")
	}
    brokerHosts := brokerhosts.([]interface{})
	if len(brokerHosts) <= 0 {
		return errors.New("broker: num of brokerHosts is zero")
	}
	for _, brokerhost := range brokerHosts {
		c.brokerServerList = append(c.brokerServerList, brokerhost.(string))
	}
	ep, ok := m ["write_disabled_partitions"]
	if ok {
        eps := ep.([]interface{})
	    if len(eps) <= 0 {
	    	return errors.New("broker: num of write_disabled_partitions is zero")
	    }
	    var epst []int
	    for _, eplinei := range eps {
            epline := eplinei.(string)
	    	epse := strings.Split(epline, "-")
	    	if len(epse) != 2 {
	    		return errors.New("broker: write_disabled_partitions foramt is wrong: should be like a-b, and both a and b are int, and a < b")
	    	}
	    	eps, err := strconv.ParseInt(epse[0], 10, 0)
	    	if err != nil {
	    		return errors.New("broker: write_disabled_partitions foramt is wrong: should be like a-b, and both a and b are int, and a < b")
	    	}
	    	epe, err := strconv.ParseInt(epse[1], 10, 0)
	    	if err != nil {
	    		return errors.New("broker: write_disabled_partitions foramt is wrong: should be like a-b, and both a and b are int, and a < b")
	    	}
	    	if eps > epe {
	    		return errors.New("broker: write_disabled_partitions foramt is wrong: should be like a-b, and both a and b are int, and a < b")
	    	}
            for i := int(eps); i <= int(epe); i ++ {
	    		epst = append(epst, i)
	    	}
	    }
	    if len(epst) == 0 {
	    	return errors.New("broker: write_disabled_partitions foramt is empty")
	    }
	    sort.Ints(epst)
	    last := epst[0]
	    c.brokerWDisabledPartitions = append(c.brokerWDisabledPartitions, last)
	    for _, v := range epst {
	    	if v == last {
	    		continue
	    	}
	        c.brokerWDisabledPartitions = append(c.brokerWDisabledPartitions, last)
	    }
	}
	ctimeo, ok := m["conn_timeout_ms"]
	if !ok {
		return errors.New("broker: conn_timeout_ms not found in conf file")
	}
	c.brokerConnTimeout = time.Duration(ctimeo.(int))*time.Millisecond
	rtimeo, ok := m["read_timeout_ms"]
	if !ok {
		return errors.New("broker: read_timeout_ms not found in conf file")
	}
	c.brokerReadTimeout = time.Duration(rtimeo.(int))*time.Millisecond
	wtimeo, ok := m["write_timeout_ms"]
	if !ok {
		return errors.New("broker: write_timeout_ms not found in conf file")
	}
	c.brokerWriteTimeout = time.Duration(wtimeo.(int))*time.Millisecond
	ms, ok := m["max_message_size"]
	if !ok {
		return errors.New("broker: max_message_size not found in conf file")
	}
	c.brokerMaxMessageSize = ms.(int)
	mrtime, ok := m["metadata_refresh_interval_ms"]
	if !ok {
		return errors.New("broker: metadata_refresh_interval_ms not found in conf file")
	}
	c.brokerMetadataRefreshInterval = time.Duration(mrtime.(int))*time.Millisecond
	//reqPoolSize, ok := m["req_pool_size"]
	//if !ok {
	//	c.cmDataPoolSize = 10240
	//	c.producerMsgPoolSize = 10240
	//} else {
	//	c.cmDataPoolSize = reqPoolSize.(int)
	//	c.producerMsgPoolSize = reqPoolSize.(int)
	//}
	return nil
}

func (c *Config) initStoreConfig() error {
	mi, ok := c.confParsed["store"]
	if !ok {
		return errors.New("store not found in conf file")
	}
    m, ok := mi.(map[interface{}]interface{})
	if !ok {
		return errors.New("store config is not map")
	}
	storehosts, ok := m["store_hosts"]
	if !ok {
		return errors.New("store_hosts not found in conf file")
	}
    storeHosts := storehosts.([]interface{})
	if len(storeHosts) <= 0 {
		return errors.New("num of storeHosts is zero")
	}
	for _, storehost := range storeHosts {
		c.storeServerList = append(c.storeServerList, storehost.(string))
	}
	timeo, ok := m["conn_timeout_ms"]
	if !ok {
		return errors.New("store conn_timeout_ms not found in conf file")
	}
	c.storeConnTimeout = time.Duration(timeo.(int))*time.Millisecond
	timeo, ok = m["socket_timeout_ms"]
	if !ok {
		return errors.New("store socket_timeout_ms not found in conf file")
	}
	c.storeSocketTimeout = time.Duration(timeo.(int))*time.Millisecond
	timeo, ok = m["operation_timeout_ms"]
	if !ok {
		return errors.New("store operation_timeout_ms not found in conf file")
	}
	c.storeOperationTimeout = time.Duration(timeo.(int))*time.Millisecond
	poolSize, ok := m["mgo_pool_size"]
	if !ok {
		return errors.New("store mgo_pool_size not found in conf file")
	}
	c.storeMgoPoolSize = poolSize.(int)
	dbName, ok := m["database_name"]
	if !ok {
		return errors.New("store database_name not found in conf file")
	}
	c.storeDbName = dbName.(string)
	collName, ok := m["collection_name"]
	if !ok {
		return errors.New("store collection_name not found in conf file")
	}
	c.storeCollName = collName.(string)
	wc, ok := m["write_concern"]
	if !ok {
		return errors.New("store write_concern not found in conf file")
	}
	c.storeWriteConcern = wc.(int)
	return nil
}

func (c *Config) initZK() error {
	mi, ok := c.confParsed["zk"]
	if !ok {
		return errors.New("zk not found in conf file")
	}
    m, ok := mi.(map[interface{}]interface{})
	if !ok {
		return errors.New("zk config is not map")
	}
	hosts, ok := m["zk_hosts"]
	if !ok {
		return errors.New("zk: zk_hosts not found in conf file")
	}
    hostsi, ok := hosts.([]interface{})
	if !ok {
		return errors.New("zk: zk_hosts not array")
	}
    for _, hosti := range hostsi {
		c.zkHosts = append(c.zkHosts, hosti.(string))
	}
    if len(c.zkHosts) == 0 {
		return errors.New("zk: num of zk_hosts is 0")
	}
	stimeo, ok := m["session_timeout_ms"]
	if !ok {
		return errors.New("zk: session_timeout_ms not found in conf file")
	}
	c.zkSessionTimeout = time.Duration(stimeo.(int))*time.Millisecond
	chroot, ok := m["zk_chroot"]
	if !ok {
		return errors.New("zk: zk_chroot not found in conf file")
	}
	c.zkChroot = chroot.(string)
	stimeo, ok = m["update_interval_ms"]
	if !ok {
		return errors.New("zk: update_interval_ms not found in conf file")
	}
	c.zkUpdateInterval = time.Duration(stimeo.(int))*time.Millisecond
	stimeo, ok = m["fail_retry_interval_ms"]
	if !ok {
		return errors.New("zk: fail_retry_interval_ms not found in conf file")
	}
	c.zkFailRetryInterval = time.Duration(stimeo.(int))*time.Millisecond
	return nil
}

func (c *Config) dump() {
	fmt.Println("confFile:", c.confFile)
	fmt.Println("logDir:", c.logDir)
	fmt.Println("logLevel:", c.logLevel)
	fmt.Println("httpServerListenPort:", c.httpServerListenPort)
	fmt.Println("httpServerReadTimeout:", c.httpServerReadTimeout)
	fmt.Println("httpServerWriteTimeout:", c.httpServerWriteTimeout)
}


