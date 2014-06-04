package whatever

import (
	"bytes"
	"strconv"
)

var (
	cmdSet     = []byte("set")
	cmdAdd     = []byte("add")
	cmdReplace = []byte("replace")
	cmdAppend  = []byte("append")
	cmdPrepend = []byte("prepend")
	cmdCas     = []byte("cas")
	cmdGet     = []byte("get")
	cmdGets    = []byte("gets")
	cmdDelete  = []byte("delete")

	strValue = []byte("VALUE")
	strEnd   = []byte("END\r\n")

	msgStored    = "STORED\r\n"
	msgNotStored = "NOT_STORED\r\n"
	msgDeleted   = "DELETED\r\n"
	msgNotFound  = "NOT_FOUND\r\n"
	msgExists    = "EXISTS\r\n"
	msgError     = "ERROR\r\n"

	maxKeyLength   = 1024
	maxValueLength = 1024 * 1024
)

type Parser struct {
	cmd         []byte
	failedToken string
	position    int
}

func (this *Parser) getNextToken() []byte {
	first := this.position

	if first += 1; first > len(this.cmd) {
		return nil
	}

	last := bytes.IndexByte(this.cmd[first:], ' ')
	if last == -1 {
		last = len(this.cmd)
	} else {
		last += first
	}

	if first == last {
		return nil
	}

	this.position = last

	return this.cmd[first:last]
}

func (this *Parser) parseUint64() (flags uint64, ok bool) {
	uintStr := this.getNextToken()
	if uintStr == nil {
		return
	}

	flags, err := strconv.ParseUint(string(uintStr[:]), 10, 64)
	if err != nil {
		return
	}

	ok = true
	return
}

func (this *Parser) parseData(size uint64) []byte {
	first := bytes.IndexAny(this.cmd, "\r\n")
	if first == -1 {
		return nil
	} else {
		first = first + 2
	}

	last := first + int(size)
	if last > len(this.cmd) {
		return nil
	}

	this.position = last

	return this.cmd[first:last]
}

func (this *Parser) parseStoreCmd(cmd []byte) (key []byte, priority uint64, flags uint64, exptime uint64, size uint64, casid uint64, ok bool) {
	this.position = len(cmd)

	key = this.getNextToken()
	if key == nil {
		this.failedToken = "key"
		return
	}

	priority, ok = this.parseUint64()
	if !ok {
		this.failedToken = "priority"
		return
	}

	flags, ok = this.parseUint64()
	if !ok {
		this.failedToken = "flags"
		return
	}

	exptime, ok = this.parseUint64()
	if !ok {
		this.failedToken = "exptime"
		return
	}

	size, ok = this.parseUint64()
	if !ok {
		this.failedToken = "size"
		return
	}

	if bytes.Equal(cmd, cmdCas) {
		casid, ok = this.parseUint64()
		if !ok {
			this.failedToken = "casid"
			return
		}
	}

	// value = this.parseData(size)
	// if value == nil {
	// 	this.failedToken = "value"
	// 	return
	// }

	ok = true
	return
}

func (this *Parser) ParseSetCmd() (key []byte, priority uint64, flags uint64, exptime uint64, size uint64, ok bool) {
	key, priority, flags, exptime, size, _, ok = this.parseStoreCmd(cmdSet)
	return
}

func (this *Parser) ParseAddCmd() (key []byte, priority uint64, flags uint64, exptime uint64, size uint64, ok bool) {
	key, priority, flags, exptime, size, _, ok = this.parseStoreCmd(cmdAdd)
	return
}

func (this *Parser) ParseReplaceCmd() (key []byte, priority uint64, flags uint64, exptime uint64, size uint64, ok bool) {
	key, priority, flags, exptime, size, _, ok = this.parseStoreCmd(cmdReplace)
	return
}

func (this *Parser) ParseAppendCmd() (key []byte, priority uint64, flags uint64, exptime uint64, size uint64, ok bool) {
	key, priority, flags, exptime, size, _, ok = this.parseStoreCmd(cmdAppend)
	return
}

func (this *Parser) ParsePrependCmd() (key []byte, priority uint64, flags uint64, exptime uint64, size uint64, ok bool) {
	key, priority, flags, exptime, size, _, ok = this.parseStoreCmd(cmdPrepend)
	return
}

func (this *Parser) ParseCasCmd() (key []byte, priority uint64, flags uint64, exptime uint64, size uint64, casid uint64, ok bool) {
	return this.parseStoreCmd(cmdCas)
}

func (this *Parser) ParseGetCmd() (key []byte, ok bool) {
	this.position = len(cmdGet)

	key = this.getNextToken()
	if key == nil {
		this.failedToken = "key"
		return
	}

	ok = true
	return
}

func (this *Parser) ParseGetsCmd() (key []byte, ok bool) {
	this.position = len(cmdGets)

	key = this.getNextToken()
	if key == nil {
		this.failedToken = "key"
		return
	}

	ok = true
	return
}

func (this *Parser) ParseDeleteCmd() (key []byte, ok bool) {
	this.position = len(cmdDelete)

	key = this.getNextToken()
	if key == nil {
		this.failedToken = "key"
		return
	}

	ok = true
	return
}

func (this *Parser) ParseGetResponse(cmd []byte) (flags uint64, size uint64, casid uint64, ok bool) {
	this.position = len(strValue)

	key := this.getNextToken()
	if key == nil {
		this.failedToken = "key"
		return
	}

	flags, ok = this.parseUint64()
	if !ok {
		this.failedToken = "flags"
		return
	}

	size, ok = this.parseUint64()
	if !ok {
		this.failedToken = "size"
		return
	}

	if bytes.Equal(cmd, cmdGets) {
		casid, ok = this.parseUint64()
		if !ok {
			this.failedToken = "casid"
			return
		}
	}

	ok = true
	return
}
