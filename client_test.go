package zanredisdb

import (
	"bytes"
	"math/rand"
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

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}

		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testKeys := make([]*PKey, 0, keysNum)
		testValues := make([][]byte, 0, keysNum)
		var writePipelines PipelineCmdList

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
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
				t.Errorf("should equal: %v, %v", val, string(testValues[i]))
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
}

func TestClientMExistsAndDel(t *testing.T) {
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

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}

		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testKeys := make([]*PKey, 0, keysNum)
		testValues := make([][]byte, 0, keysNum)
		var writePipelines PipelineCmdList

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			testKeys = append(testKeys, pk)
			rawKey := pk.RawKey
			testValue := pk.ShardingKey()
			writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
			testValues = append(testValues, testValue)
		}

		existsCnt, err := zanClient.KVMExists(true, testKeys...)
		if err != nil {
			t.Errorf("failed exists: %v", err)
		}
		if existsCnt != int64(0) {
			t.Errorf("exists count %v should be 0 for init", existsCnt)
			return
		}

		rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
		if len(rsps) != len(writePipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
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

		existsCnt, err = zanClient.KVMExists(true, testKeys...)
		if err != nil {
			t.Errorf("failed exists: %v", err)
		}
		if existsCnt != int64(len(testKeys)) {
			t.Errorf("exists count %v mismatch keys: %v", existsCnt, len(testKeys))
		}

		delCnt, err := zanClient.KVMDel(true, testKeys...)
		if err != nil {
			t.Errorf("failed del: %v", err)
		}
		if delCnt != int64(len(testKeys)) {
			t.Errorf("del cnt %v should equal keys: %v", delCnt, len(testKeys))
		}

		existsCnt, err = zanClient.KVMExists(true, testKeys...)
		if err != nil {
			t.Errorf("failed exists: %v", err)
		}
		if existsCnt != int64(0) {
			t.Errorf("exists count %v mismatch keys: %v", existsCnt, len(testKeys))
		}

		delCnt, err = zanClient.KVMDel(true, testKeys...)
		if err != nil {
			t.Errorf("failed del: %v", err)
		}
		if delCnt != int64(0) {
			t.Errorf("del cnt %v should be 0 after deleted", delCnt)
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

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}
		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testValues := make([][]byte, 0, keysNum)
		var writePipelines PipelineCmdList

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			rawKey := pk.RawKey
			testValue := pk.ShardingKey()
			writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
			testValues = append(testValues, testValue)
		}
		rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
		if len(rsps) != len(writePipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
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
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
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
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
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
}
