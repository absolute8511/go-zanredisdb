package zanredisdb

import (
	"bytes"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"testing"
	"time"
)

var pdAddr = "127.0.0.1:18001"
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
		DialTimeout:  time.Second * 5,
		ReadTimeout:  time.Second * 5,
		WriteTimeout: time.Second * 5,
		TendInterval: 3,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	SetLogger(2, newTestLogger(t))
	cluster := NewCluster(conf)
	defer cluster.Close()
	nodeNum := len(cluster.nodes)
	if nodeNum == 0 {
		t.Fatal("cluster nodes should not empty")
	}
	time.Sleep(time.Second * time.Duration(conf.TendInterval*2))
	if nodeNum != len(cluster.nodes) {
		t.Fatal("cluster nodes should not changed")
	}
	conn, err := cluster.GetConn([]byte("11"), true)
	if err != nil {
		t.Error(err)
	}
	_, err = conn.Do("PING")
	if err != nil {
		t.Error(err)
	}
}

func TestClusterReadWrite(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 5,
		ReadTimeout:  time.Second * 5,
		WriteTimeout: time.Second * 5,
		TendInterval: 3,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	SetLogger(2, newTestLogger(t))
	cluster := NewCluster(conf)
	defer cluster.Close()
	nodeNum := len(cluster.nodes)
	if nodeNum == 0 {
		t.Fatal("cluster nodes should not empty")
	}
	for i := 0; i < 10; i++ {
		pk := NewPKey(conf.Namespace, "unittest", []byte("rw11"+strconv.Itoa(i)))
		conn, err := cluster.GetConn(pk.ShardingKey(), true)
		if err != nil {
			t.Error(err)
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
