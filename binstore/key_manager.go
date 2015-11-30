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
		"github.com/garyburd/redigo/redis"
		"../../fcrypt"
		"math/rand"
		"time"
		"fmt"
		"errors"
		"strings"
	   )

var (
	gIdMaxValue = uint64(1<<64 - 1)
	gPartitionMaxValue = int32(1<<10 -1)
	gOffsetMaxValue = int64(1<<54 - 1)
)

type KeyManager struct {
	config *Config
	fc *fcrypt.FCrypt
	idRedis [2]*redis.Pool
	idKey string    // ${keyTag}_idalloc
	keyTag string
	keyTagLen int
}

type IdRedisDialer struct {
	addr string
	connTimeout time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration
}

func (rd *IdRedisDialer) dial() (redis.Conn, error) {
	return redis.DialTimeout("tcp", rd.addr, rd.connTimeout, rd.readTimeout, rd.writeTimeout)
}

func (rd *IdRedisDialer) testOnBorrow(c redis.Conn, t time.Time) error {
	_, err := c.Do("PING")
	return err
}

func newKeyManager(bs *BinStore) (*KeyManager, error) {
    km := &KeyManager{
        config: bs.config,
	}
	err := km.init()
	if err != nil {
		return nil, err
	}
	return km, nil
}

func (km *KeyManager) init() error {
    err := km.initFCrypt()
	if err != nil {
		return err
	}
	err = km.initIdRedis()
	if err != nil {
		return err
	}
	return nil
}

func (km *KeyManager) initFCrypt() error {
	km.fc = fcrypt.NewFCrypt(km.config.kmFCryptKey)
	km.keyTag = km.config.kmKeyTag
	km.keyTagLen = len(km.keyTag)
	return nil
}

func (km *KeyManager) initIdRedis() error {
	km.idRedis[0] = km.initIdRedisOne(km.config.kmEvenRedisAddr)
	km.idRedis[1] = km.initIdRedisOne(km.config.kmOddRedisAddr)
	km.idKey = fmt.Sprintf("%s_idalloc", km.config.kmKeyTag)
	return nil
}

func (km *KeyManager) initIdRedisOne(addr string) *redis.Pool {
    ord := &IdRedisDialer {
        addr: addr,
		connTimeout: km.config.kmRedisConnTimeout,
		readTimeout: km.config.kmRedisReadTimeout,
		writeTimeout: km.config.kmRedisWriteTimeout,
	}
    kmRedis := &redis.Pool {
        MaxIdle: km.config.kmRedisMinConnEach,
		MaxActive: km.config.kmRedisMaxConnEach,
		IdleTimeout: km.config.kmRedisPoolIdleTimeout,
		Dial: ord.dial,
		TestOnBorrow: ord.testOnBorrow,
	}
	return kmRedis
}

func (km *KeyManager) getNewId() (uint64, error) {
    i := rand.Int()%2
	id, err := km.getNewIdIdx(i)
	if err == nil {
		return id, nil
	}
	return km.getNewIdIdx((i+1)%2)
}

func (km *KeyManager) getNewIdIdx(idx int) (uint64, error) {
	conn := km.idRedis[idx].Get()
	defer conn.Close()
	r, err := conn.Do("INCR", km.idKey)
	if err != nil {
		return 0, err
	}
	id, err := redis.Uint64(r, err)
	if err != nil {
		return 0, err
	}
	return id*2+uint64(idx), nil
}

func (km *KeyManager) generateKey(id uint64, partition int32, offset int64) (string, error) {
	if id > gIdMaxValue {
		return "", errors.New(fmt.Sprintf("max id shall be %d, now is %d", gIdMaxValue, id))
	}
	if partition > gPartitionMaxValue {
		return "", errors.New(fmt.Sprintf("max partition shall be %d, partition %d has exceeded it", gPartitionMaxValue, partition))
	}
	if offset > gOffsetMaxValue {
		return "", errors.New(fmt.Sprintf("max offset shall be %d, partition %d has exceeded it", gOffsetMaxValue, partition))
	}
    id2 := uint64(partition << 54) + uint64(offset)
	key, err := km.fc.Id64ToHstr(id, id2)
	if err != nil {
		return "", errors.New(fmt.Sprintf("fail to fcrypt.Id64ToHstr: %s", err.Error()))
	}
	return fmt.Sprintf("%s%s", km.keyTag, key), nil
}

func (km *KeyManager) parseKey(key string) (uint64, int32, int64, error) {
	if !strings.HasPrefix(key, km.keyTag) {
		return 0,0,0,errors.New("key dose not contains key tag")
	}
    basicKey := strings.TrimPrefix(key, km.keyTag)
	id1, id2, err := km.fc.HstrToId64(basicKey)
	if err != nil {
		return 0,0,0,err
	}
	offset := int64(id2 & 0x3fffffffffffff)
	partition := int32(id2 >> 64)
	return id1, partition, offset, nil
}
