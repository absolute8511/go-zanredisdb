package zanredisdb

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/redigo/redis"
)

var pdAddr = "qabb-qa-zankv0:18001"
var testNS = "yz_test_p4"

type testLogger struct {
	t *testing.T
}

func (self *testLogger) Output(depth int, s string) {
	self.t.Log(s)
}

func (self *testLogger) OutputErr(depth int, s string) {
	self.t.Log(s)
}

func (self *testLogger) Flush() {
}
func newTestLogger(t *testing.T) *testLogger {
	return &testLogger{t}
}

func TestClusterInfo(t *testing.T) {
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
	SetLogger(int32(logLevel), newTestLogger(t))
	cluster := NewCluster(conf)
	defer cluster.Close()
	nodeNum := len(cluster.getPartitions().PList)
	if nodeNum == 0 {
		t.Fatal("cluster nodes should not empty")
	}
	time.Sleep(time.Second * time.Duration(conf.TendInterval*2))
	if nodeNum != len(cluster.getPartitions().PList) {
		t.Fatal("cluster nodes should not changed")
	}
	conn, err := cluster.GetConn([]byte("11"), true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Do("PING")
	if err != nil {
		t.Error(err)
	}
}

func TestClusterReadWrite(t *testing.T) {
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
	cluster := NewCluster(conf)
	defer cluster.Close()
	nodeNum := len(cluster.getPartitions().PList)
	if nodeNum == 0 {
		t.Fatal("cluster nodes should not empty")
	}
	for i := 0; i < 10; i++ {
		pk := NewPKey(conf.Namespace, "unittest", []byte("rw11"+strconv.Itoa(i)))
		conn, err := cluster.GetConn(pk.ShardingKey(), true)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		rawKey := pk.RawKey
		conn.Do("DEL", rawKey)
		value, err := redis.Bytes(conn.Do("GET", rawKey))
		if err != redis.ErrNil && len(value) > 0 {
			t.Fatalf("should be deleted:%v, value:%v", err, value)
		}

		testValue := pk.ShardingKey()
		_, err = redis.String(conn.Do("SET", rawKey, testValue))
		if err != nil {
			t.Fatal(err)
		}
		value, err = redis.Bytes(conn.Do("GET", rawKey))
		if err != nil {
			t.Error(err)
		} else if !bytes.Equal(value, testValue) {
			t.Errorf("should equal: %v, %v", value, testValue)
		}
		conn.Do("DEL", rawKey)
	}
}

func TestClusterRemoveFailedLookup(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	failedSeedLookup := "127.0.0.1:4111"
	conf.LookupList = append(conf.LookupList, pdAddr)
	conf.LookupList = append(conf.LookupList, failedSeedLookup)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	cluster := NewCluster(conf)
	defer cluster.Close()
	nodeNum := len(cluster.getPartitions().PList)
	if nodeNum == 0 {
		t.Fatal("cluster nodes should not empty")
	}
	cluster.lookupMtx.Lock()
	for _, addr := range conf.LookupList {
		if FindString(cluster.LookupList, addr) == -1 {
			t.Errorf("cluster lookup seed should be the same")
		}
	}
	failedLookup := "127.0.0.1:3111"
	cluster.LookupList = append(cluster.LookupList, failedLookup)
	cluster.lookupMtx.Unlock()

	time.Sleep(time.Second * time.Duration(conf.TendInterval*3))
	if nodeNum != len(cluster.getPartitions().PList) {
		t.Fatal("cluster nodes should not changed")
	}

	time.Sleep(time.Second * time.Duration(conf.TendInterval*5))
	cluster.lookupMtx.Lock()
	defer cluster.lookupMtx.Unlock()
	for _, addr := range conf.LookupList {
		if FindString(cluster.LookupList, addr) == -1 {
			t.Errorf("cluster lookup seed should be the same")
		}
	}

	if FindString(cluster.LookupList, failedLookup) != -1 {
		t.Errorf("failed lookup should be removed")
	}
}

func BenchmarkGetNodePool(b *testing.B) {
	b.StopTimer()
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	cluster := NewCluster(conf)
	defer cluster.Close()
	nodeNum := len(cluster.getPartitions().PList)
	if nodeNum == 0 {
		b.Fatal("cluster nodes should not empty")
	}
	b.StartTimer()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				pk := NewPKey(conf.Namespace, "unittest", []byte("rw11"+strconv.Itoa(i)))
				conn, err := cluster.GetConn(pk.ShardingKey(), true)
				if err != nil {
					b.Error(err)
					break
				}
				conn.Close()
			}
		}()
	}

	wg.Wait()
}
