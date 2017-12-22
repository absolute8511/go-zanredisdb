package zanredisdb

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/absolute8511/redigo/redis"
)

func TestClientMget(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient := NewZanRedisClient(conf)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)
	testKeys := make([]*PKey, 0, 10)
	testValues := make([][]byte, 0, 10)
	var writePipelines PipelineCmdList
	for i := 0; i < 10; i++ {
		pk := NewPKey(conf.Namespace, "unittest", []byte("rw11"+strconv.Itoa(i)))
		testKeys = append(testKeys, pk)
		rawKey := pk.RawKey
		testValue := pk.ShardingKey()
		writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
		testValues = append(testValues, testValue)
	}
	rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
	if len(rsps) != len(writePipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
	}
	if len(rsps) != 10 {
		t.Errorf("rsp list should be equal to command number %v", len(rsps))
	}
	t.Logf("test values: %v", len(testValues))
	for i, rsp := range rsps {
		if errs[i] != nil {
			t.Errorf("rsp error: %v", errs[i])
		}
		_, err := redis.String(rsp, errs[i])
		if err != nil {
			t.Error(err)
		}
	}

	mgetVals, err := zanClient.KVMGet(true, testKeys...)
	if err != nil {
		t.Errorf("failed mget: %v", err)
	}
	if len(mgetVals) != len(testKeys) {
		t.Errorf("mget count mismatch: %v %v", len(mgetVals), len(testKeys))
	}
	for i, val := range mgetVals {
		t.Log(string(val))
		if !bytes.Equal(val, testValues[i]) {
			t.Errorf("should equal: %v, %v", val, testValues[i])
		}
	}
	var delPipelines PipelineCmdList
	for i := 0; i < len(testKeys); i++ {
		delPipelines.Add("DEL", testKeys[i].ShardingKey(), true, testKeys[i].RawKey)
	}
	rsps, errs = zanClient.FlushAndWaitPipelineCmd(delPipelines)
	if len(rsps) != len(delPipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(delPipelines))
	}

	mgetVals, err = zanClient.KVMGet(true, testKeys...)
	if err != nil {
		t.Errorf("failed mget: %v", err)
	}
	if len(mgetVals) != len(testKeys) {
		t.Errorf("mget count mismatch: %v %v", len(mgetVals), len(testKeys))
	}
	t.Log(mgetVals)
	for _, val := range mgetVals {
		if len(val) > 0 {
			t.Errorf("should empty after del: %v", val)
		}
	}
}

func TestClientPipeline(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient := NewZanRedisClient(conf)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)
	var writePipelines PipelineCmdList
	testValues := make([][]byte, 0, 10)
	for i := 0; i < 10; i++ {
		pk := NewPKey(conf.Namespace, "unittest", []byte("rw11"+strconv.Itoa(i)))
		rawKey := pk.RawKey
		testValue := pk.ShardingKey()
		writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
		testValues = append(testValues, testValue)
	}
	rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
	if len(rsps) != len(writePipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
	}
	if len(rsps) != 10 {
		t.Errorf("rsp list should be equal to command number %v", len(rsps))
	}
	t.Logf("test values: %v", len(testValues))
	for i, rsp := range rsps {
		if errs[i] != nil {
			t.Errorf("rsp error: %v", errs[i])
		}
		_, err := redis.String(rsp, errs[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	var getPipelines PipelineCmdList
	for i := 0; i < len(testValues); i++ {
		pk := NewPKey(conf.Namespace, "unittest", []byte("rw11"+strconv.Itoa(i)))
		rawKey := pk.RawKey
		getPipelines.Add("GET", pk.ShardingKey(), true, rawKey)
	}
	rsps, errs = zanClient.FlushAndWaitPipelineCmd(getPipelines)
	if len(rsps) != len(getPipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(getPipelines))
	}
	for i, rsp := range rsps {
		if errs[i] != nil {
			t.Errorf("rsp error: %v", errs[i])
		}
		value, err := redis.Bytes(rsp, errs[i])
		if err != nil {
			t.Error(err)
		} else if !bytes.Equal(value, testValues[i]) {
			t.Errorf("should equal: %v, %v", value, testValues[i])
		}
	}
	var delPipelines PipelineCmdList
	for i := 0; i < len(testValues); i++ {
		pk := NewPKey(conf.Namespace, "unittest", []byte("rw11"+strconv.Itoa(i)))
		rawKey := pk.RawKey
		delPipelines.Add("DEL", pk.ShardingKey(), true, rawKey)
	}
	rsps, errs = zanClient.FlushAndWaitPipelineCmd(delPipelines)
	if len(rsps) != len(delPipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(delPipelines))
	}
	rsps, errs = zanClient.FlushAndWaitPipelineCmd(getPipelines)
	if len(rsps) != len(getPipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(getPipelines))
	}
	for i, rsp := range rsps {
		if errs[i] != nil {
			t.Errorf("rsp error: %v", errs[i])
		}
		value, err := redis.Bytes(rsp, errs[i])
		if err != redis.ErrNil && len(value) > 0 {
			t.Fatalf("should be deleted:%v, value:%v", err, string(value))
		}
	}
}
