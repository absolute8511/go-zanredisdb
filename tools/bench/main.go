package main

import (
	"flag"
	"fmt"
	"github.com/absolute8511/go-zanredisdb"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ip = flag.String("ip", "127.0.0.1", "pd server ip")
var port = flag.Int("port", 18001, "pd server port")
var number = flag.Int("n", 1000, "request number")
var clients = flag.Int("c", 10, "number of clients")
var round = flag.Int("r", 1, "benchmark round number")
var valueSize = flag.Int("vsize", 100, "kv value size")
var tests = flag.String("t", "set,get,randget,del", "only run the comma separated list of tests")
var primaryKeyCnt = flag.Int("pkn", 100, "primary key count for hash,list,set,zset")
var namespace = flag.String("namespace", "default", "the prefix namespace")
var table = flag.String("table", "test", "the table to write")

var wg sync.WaitGroup
var loop int = 0

func doCommand(client *zanredisdb.ZanRedisClient, cmd string, args ...interface{}) error {
	v := args[0]
	prefix := *namespace + ":" + *table + ":"
	sharding := ""
	switch vt := v.(type) {
	case string:
		sharding = *table + ":" + vt
		args[0] = prefix + vt
	case []byte:
		sharding = *table + ":" + string(vt)
		args[0] = []byte(prefix + string(vt))
	case int:
		sharding = *table + ":" + strconv.Itoa(vt)
		args[0] = prefix + strconv.Itoa(vt)
	case int64:
		sharding = *table + ":" + strconv.Itoa(int(vt))
		args[0] = prefix + strconv.Itoa(int(vt))
	}
	_, err := client.DoRedis(strings.ToUpper(cmd), []byte(sharding), true, args...)
	if err != nil {
		fmt.Printf("do %s (%v) error %s\n", cmd, args[0], err.Error())
		return err
	}
	return nil
}

func bench(cmd string, f func(c *zanredisdb.ZanRedisClient) error) {
	wg.Add(*clients)

	t1 := time.Now()
	pdAddr := fmt.Sprintf("%s:%d", *ip, *port)
	for i := 0; i < *clients; i++ {
		go func() {
			var err error
			conf := &zanredisdb.Conf{
				DialTimeout:  time.Second * 5,
				ReadTimeout:  time.Second * 5,
				WriteTimeout: time.Second * 5,
				TendInterval: 10,
				Namespace:    *namespace,
			}
			conf.LookupList = append(conf.LookupList, pdAddr)
			c := zanredisdb.NewZanRedisClient(conf)
			c.Start()
			for j := 0; j < loop; j++ {
				err = f(c)
				if err != nil {
					break
				}
			}
			c.Stop()
			wg.Done()
		}()
	}

	wg.Wait()
	t2 := time.Now()
	d := t2.Sub(t1)

	fmt.Printf("%s: %s %0.3f micros/op, %0.2fop/s\n",
		cmd,
		d.String(),
		float64(d.Nanoseconds()/1e3)/float64(*number),
		float64(*number)/d.Seconds())
}

var kvSetBase int64 = 0
var kvGetBase int64 = 0
var kvIncrBase int64 = 0
var kvDelBase int64 = 0

func benchSet() {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}
	magicIdentify := make([]byte, 9+3+3)
	for i := 0; i < len(magicIdentify); i++ {
		if i < 3 || i > len(magicIdentify)-3 {
			magicIdentify[i] = 0
		} else {
			magicIdentify[i] = byte(i % 3)
		}
	}
	f := func(c *zanredisdb.ZanRedisClient) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&kvSetBase, 1)
		tmp := fmt.Sprintf("%010d", int(n))
		ts := time.Now().String()
		index := 0
		copy(value[index:], magicIdentify)
		index += len(magicIdentify)
		if index < *valueSize {
			copy(value[index:], ts)
			index += len(ts)
		}
		if index < *valueSize {
			copy(value[index:], tmp)
			index += len(tmp)
		}
		if *valueSize > len(magicIdentify) {
			copy(value[len(value)-len(magicIdentify):], magicIdentify)
		}
		return doCommand(c, "SET", tmp, value)
	}
	bench("set", f)
}

func benchGet() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&kvGetBase, 1)
		return doCommand(c, "GET", n)
	}
	bench("get", f)
}

func benchRandGet() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := rand.Int() % *number
		return doCommand(c, "GET", n)
	}
	bench("randget", f)
}

func benchDel() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&kvDelBase, 1)
		return doCommand(c, "DEL", n)
	}

	bench("del", f)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *number <= 0 {
		panic("invalid number")
		return
	}

	if *clients <= 0 || *number < *clients {
		panic("invalid client number")
		return
	}

	loop = *number / *clients

	if *round <= 0 {
		*round = 1
	}

	zanredisdb.SetLogger(1, zanredisdb.NewSimpleLogger())
	ts := strings.Split(*tests, ",")
	for i := 0; i < *round; i++ {
		for _, s := range ts {
			switch strings.ToLower(s) {
			case "set":
				benchSet()
			case "get":
				benchGet()
			case "randget":
				benchRandGet()
			case "del":
				benchDel()
			}
		}
		println("")
	}
}
