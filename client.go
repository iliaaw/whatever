package whatever

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net"
	"sort"
	"strconv"
	"sync"
)

type Client struct {
	addrs       []int
	m           map[int]net.Addr
	parser      *Parser
	connections map[string][]*net.Conn
	mutex       sync.Mutex
}

const (
	pointCount              = 5
	maxConnectionsPerServer = 10
)

func NewClient() *Client {
	client := new(Client)
	client.parser = new(Parser)
	client.m = make(map[int]net.Addr)
	client.connections = make(map[string][]*net.Conn)

	return client
}

func (this *Client) AddServer(addr string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	address, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}

	for i := 0; i < pointCount; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(addr + strconv.Itoa(i))))
		this.addrs = append(this.addrs, hash)
		this.m[hash] = address
	}

	sort.Ints(this.addrs)

	return nil
}

func (this *Client) getServerAddr(key []byte) net.Addr {
	if len(this.addrs) == 0 {
		return nil
	}

	hash := int(crc32.ChecksumIEEE(key))
	for _, h := range this.addrs {
		if h > hash {
			return this.m[h]
		}
	}

	return this.m[this.addrs[0]]
}

func (this *Client) getConnection(addr net.Addr) (conn *net.Conn, err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	connections, ok := this.connections[addr.String()]
	if !ok {
		this.connections[addr.String()] = make([]*net.Conn, 0)
	}

	if len(connections) == 0 {
		c, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			return nil, err
		}
		conn = &c
	} else {
		conn = connections[len(connections)-1]
		this.connections[addr.String()] = connections[:len(connections)-1]
	}

	return conn, nil
}

func (this *Client) releaseConnection(addr net.Addr, conn *net.Conn) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.connections[addr.String()] = append(this.connections[addr.String()], conn)
}

func (this *Client) store(cmd []byte, key []byte, priority uint64, flags uint64, exptime uint64, casid uint64, value []byte) (err error) {
	if err = this.validate(key, value); err != nil {
		return
	}

	addr := this.getServerAddr(key)
	if addr == nil {
		return fmt.Errorf("No servers added")
	}

	conn, err := this.getConnection(addr)
	if err != nil {
		return err
	}
	defer this.releaseConnection(addr, conn)

	rw := bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn))

	if bytes.Equal(cmd, cmdCas) {
		if _, err = fmt.Fprintf(rw, "%s %s %d %d %d %d %d \r\n", cmd, key, priority, flags, exptime, len(value), casid); err != nil {
			return
		}
	} else {
		if _, err = fmt.Fprintf(rw, "%s %s %d %d %d %d \r\n", cmd, key, priority, flags, exptime, len(value)); err != nil {
			return
		}
	}

	if _, err = rw.Write(value); err != nil {
		return
	}

	if err = rw.Flush(); err != nil {
		return
	}

	line, err := rw.ReadSlice('\n')
	if err != nil {
		return
	}
	switch {
	case bytes.Equal(line, []byte(msgStored)):
		return nil
	case bytes.Equal(line, []byte(msgNotStored)):
		return fmt.Errorf("Not stored")
	case bytes.Equal(line, []byte(msgNotFound)):
		return fmt.Errorf("Not found")
	case bytes.Equal(line, []byte(msgExists)):
		return fmt.Errorf("Exists")
	}

	return fmt.Errorf("Unexpected response")
}

func (this *Client) Set(key []byte, priority uint64, flags uint64, exptime uint64, value []byte) error {
	return this.store(cmdSet, key, priority, flags, exptime, 0, value)
}

func (this *Client) Add(key []byte, priority uint64, flags uint64, exptime uint64, value []byte) error {
	return this.store(cmdAdd, key, priority, flags, exptime, 0, value)
}

func (this *Client) Replace(key []byte, priority uint64, flags uint64, exptime uint64, value []byte) error {
	return this.store(cmdReplace, key, priority, flags, exptime, 0, value)
}

func (this *Client) Append(key []byte, priority uint64, flags uint64, exptime uint64, value []byte) error {
	return this.store(cmdAppend, key, priority, flags, exptime, 0, value)
}

func (this *Client) Prepend(key []byte, priority uint64, flags uint64, exptime uint64, value []byte) error {
	return this.store(cmdPrepend, key, priority, flags, exptime, 0, value)
}

func (this *Client) Cas(key []byte, priority uint64, flags uint64, exptime uint64, casid uint64, value []byte) error {
	return this.store(cmdCas, key, priority, flags, exptime, casid, value)
}

func (this *Client) get(cmd []byte, key []byte) (value []byte, flags uint64, casid uint64, err error) {
	if err = this.validate(key, nil); err != nil {
		return
	}

	addr := this.getServerAddr(key)
	if addr == nil {
		err = fmt.Errorf("No servers added")
		return
	}

	conn, err := this.getConnection(addr)
	if err != nil {
		return
	}
	defer this.releaseConnection(addr, conn)

	rw := bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn))

	if _, err = fmt.Fprintf(rw, "%s %s\r\n", cmd, key); err != nil {
		return
	}

	if err = rw.Flush(); err != nil {
		return
	}

	for {
		var line []byte
		line, err = rw.ReadSlice('\n')
		if err == io.EOF {
			err = nil
			return
		}
		if err != nil {
			return
		}

		if bytes.Equal(line, strEnd) {
			return
		}

		this.parser.cmd = line
		var size uint64
		var ok bool
		flags, size, casid, ok = this.parser.ParseGetResponse(cmd)

		if !ok {
			err = fmt.Errorf("Cannot parse %s", this.parser.failedToken)
			return
		}

		value, err = ioutil.ReadAll(io.LimitReader(rw, int64(size)))
		if err != nil {
			return
		}

		return value, flags, casid, nil
	}
}

func (this *Client) Get(key []byte) (value []byte, flags uint64, err error) {
	value, flags, _, err = this.get(cmdGet, key)
	return
}

func (this *Client) Gets(key []byte) (value []byte, flags uint64, casid uint64, err error) {
	return this.get(cmdGets, key)
}

func (this *Client) Delete(key []byte) (err error) {
	if err = this.validate(key, nil); err != nil {
		return
	}

	addr := this.getServerAddr(key)
	if addr == nil {
		return fmt.Errorf("No servers added")
	}

	conn, err := this.getConnection(addr)
	if err != nil {
		return
	}
	defer this.releaseConnection(addr, conn)

	rw := bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn))

	if _, err = fmt.Fprintf(rw, "%s %s \r\n", cmdDelete, key); err != nil {
		return
	}

	if err = rw.Flush(); err != nil {
		return
	}

	line, err := rw.ReadSlice('\n')
	if err != nil {
		return
	}
	switch {
	case bytes.Equal(line, []byte(msgDeleted)):
		return nil
	case bytes.Equal(line, []byte(msgNotFound)):
		return fmt.Errorf("Not found")
	}

	return fmt.Errorf("Unextected response")
}

func (this *Client) validate(key []byte, value []byte) (err error) {
	if len(key) == 0 || len(key) > maxKeyLength {
		return fmt.Errorf("Invalid key")
	}

	if value != nil && (len(value) == 0 || len(value) > maxValueLength) {
		return fmt.Errorf("Invalid value")
	}

	return nil
}
