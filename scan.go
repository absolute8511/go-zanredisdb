package zanredisdb

import (
	"bytes"
	"fmt"
	"strconv"
)

type ScanKey struct {
	Namespace string
	Type      string
	Set       string
	Count     int
	Cursor    []byte
	RawKey    []byte
}

func NewScanKey(ns, set, t string, count int, cursor []byte) *ScanKey {
	var tmp bytes.Buffer
	tmp.WriteString(ns)
	tmp.WriteString(":")
	tmp.WriteString(set)
	tmp.WriteString(":")
	tmp.Write(cursor)
	if count > 0 {
		tmp.WriteString(" count ")
		tmp.WriteString(strconv.Itoa(count))
	}
	return &ScanKey{
		Namespace: ns,
		Set:       set,
		Type:      t,
		Count:     count,
		Cursor:    cursor,
		RawKey:    tmp.Bytes(),
	}
}

func (self *ScanKey) String() string {
	return fmt.Sprintf("%s", string(self.RawKey))
}
