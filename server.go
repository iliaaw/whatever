package whatever

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
)

type Server struct {
	addr     string
	cache    *Cache
	parser   *Parser
	response string
	socket   *net.TCPListener
}

func NewServer(addr string, verbose bool, maxLength int) *Server {
	server := new(Server)
	server.addr = addr
	server.cache = NewCache(maxLength)
	server.parser = new(Parser)

	if !verbose {
		log.SetOutput(ioutil.Discard)
	}

	return server
}

func (this *Server) Start() {
	address, err := net.ResolveTCPAddr("tcp", this.addr)
	if err != nil {
		log.Fatalf("Cannot resolve address %s: %s", address, err)
	}

	this.socket, err = net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("Cannot bind to address %s: %s", address, err)
	}

	for {
		conn, err := this.socket.AcceptTCP()
		if err != nil {
			log.Fatalf("Cannot accept TCP connection: %s", err)
		}

		go this.handleConn(conn)
	}
}

func (this *Server) Stop() {
	this.socket.Close()
	this.socket = nil
}

func (this *Server) handleConn(conn net.Conn) {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		var buffer []byte
		buffer, err := rw.ReadSlice('\n')
		if err == io.EOF {
			return
		} else if err != nil {
			log.Fatalf("Cannot read TCP connection data: %s", err)
		}

		this.parser.cmd = bytes.Trim(buffer[:], "\r\n")

		if bytes.HasPrefix(buffer, cmdSet) {
			log.Printf("Received «set» command: \"%s\"", this.parser.cmd)
			this.runSetCmd(rw)
		} else if bytes.HasPrefix(buffer, cmdAdd) {
			log.Printf("Received «add» command: \"%s\"", this.parser.cmd)
			this.runAddCmd(rw)
		} else if bytes.HasPrefix(buffer, cmdReplace) {
			log.Printf("Received «replace» command: \"%s\"", this.parser.cmd)
			this.runReplaceCmd(rw)
		} else if bytes.HasPrefix(buffer, cmdAppend) {
			log.Printf("Received «append» command: \"%s\"", this.parser.cmd)
			this.runAppendCmd(rw)
		} else if bytes.HasPrefix(buffer, cmdPrepend) {
			log.Printf("Received «prepend» command: \"%s\"", this.parser.cmd)
			this.runPrependCmd(rw)
		} else if bytes.HasPrefix(buffer, cmdCas) {
			log.Printf("Received «cas» command: \"%s\"", this.parser.cmd)
			this.runCasCmd(rw)
		} else if bytes.HasPrefix(buffer, cmdGets) {
			log.Printf("Received «gets» command: %s", this.parser.cmd)
			this.runGetsCmd()
		} else if bytes.HasPrefix(buffer, cmdGet) {
			log.Printf("Received «get» command: %s", this.parser.cmd)
			this.runGetCmd()
		} else if bytes.HasPrefix(buffer, cmdDelete) {
			log.Printf("Received «delete» command: %s", this.parser.cmd)
			this.runDeleteCmd()
		} else if len(buffer) == 1 {
			return
		} else {
			log.Printf("Received nonexistent command «%s»", this.parser.cmd)
			this.handleError()
		}

		if _, err = rw.Write([]byte(this.response)); err != nil {
			return
		}

		if err = rw.Flush(); err != nil {
			return
		}

		this.response = ""
	}
}

func (this *Server) runSetCmd(rw *bufio.ReadWriter) {
	key, priority, flags, exptime, size, ok := this.parser.ParseSetCmd()
	if !ok {
		log.Printf("An error occured while parsing «set» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	value, err := ioutil.ReadAll(io.LimitReader(rw, int64(size)))
	if err != nil {
		return
	}

	log.Printf("Parsed «set» command arguments: key=\"%s\", value=\"%s\", priority=\"%d\", flags=\"%d\", exptime=\"%d\"", key, value, priority, flags, exptime)

	this.cache.Set(string(key[:]), value, priority, flags, exptime)
	this.response = msgStored
}

func (this *Server) runAddCmd(rw *bufio.ReadWriter) {
	key, priority, flags, exptime, size, ok := this.parser.ParseAddCmd()
	if !ok {
		log.Printf("An error occured while parsing «add» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	value, err := ioutil.ReadAll(io.LimitReader(rw, int64(size)))
	if err != nil {
		return
	}

	log.Printf("Parsed «add» command arguments: key=\"%s\", value=\"%s\", priority=\"%d\", flags=\"%d\", exptime=\"%d\"", key, value, priority, flags, exptime)
	if ok = this.cache.Add(string(key[:]), value, priority, flags, exptime); ok {
		this.response = msgStored
	} else {
		this.response = msgNotStored
	}
}

func (this *Server) runReplaceCmd(rw *bufio.ReadWriter) {
	key, priority, flags, exptime, size, ok := this.parser.ParseReplaceCmd()
	if !ok {
		log.Printf("An error occured while parsing «replace» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	value, err := ioutil.ReadAll(io.LimitReader(rw, int64(size)))
	if err != nil {
		return
	}

	log.Printf("Parsed «replace» command arguments: key=\"%s\", value=\"%s\", priority=\"%d\", flags=\"%d\", exptime=\"%d\"", key, value, priority, flags, exptime)
	if ok = this.cache.Replace(string(key[:]), value, priority, flags, exptime); ok {
		this.response = msgStored
	} else {
		this.response = msgNotStored
	}
}

func (this *Server) runAppendCmd(rw *bufio.ReadWriter) {
	key, priority, flags, exptime, size, ok := this.parser.ParseAppendCmd()
	if !ok {
		log.Printf("An error occured while parsing «append» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	value, err := ioutil.ReadAll(io.LimitReader(rw, int64(size)))
	if err != nil {
		return
	}

	log.Printf("Parsed «append» command arguments: key=\"%s\", value=\"%s\", priority=\"%d\", flags=\"%d\", exptime=\"%d\"", key, value, priority, flags, exptime)
	if ok = this.cache.Append(string(key[:]), value, priority, flags, exptime); ok {
		this.response = msgStored
	} else {
		this.response = msgNotStored
	}
}

func (this *Server) runPrependCmd(rw *bufio.ReadWriter) {
	key, priority, flags, exptime, size, ok := this.parser.ParsePrependCmd()
	if !ok {
		log.Printf("An error occured while parsing «prepend» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	value, err := ioutil.ReadAll(io.LimitReader(rw, int64(size)))
	if err != nil {
		return
	}

	log.Printf("Parsed «prepend» command arguments: key=\"%s\", value=\"%s\", priority=\"%d\", flags=\"%d\", exptime=\"%d\"", key, value, priority, flags, exptime)
	if ok = this.cache.Prepend(string(key[:]), value, priority, flags, exptime); ok {
		this.response = msgStored
	} else {
		this.response = msgNotStored
	}
}

func (this *Server) runCasCmd(rw *bufio.ReadWriter) {
	key, priority, flags, exptime, size, casid, ok := this.parser.ParseCasCmd()
	if !ok {
		log.Printf("An error occured while parsing «cas» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	value, err := ioutil.ReadAll(io.LimitReader(rw, int64(size)))
	if err != nil {
		return
	}

	log.Printf("Parsed «cas» command arguments: key=\"%s\", value=\"%s\", priority=\"%d\", flags=\"%d\", exptime=\"%d\", casid=\"%d\"", key, value, priority, flags, exptime, casid)
	if entry, ok := this.cache.CheckAndStore(string(key[:]), value, priority, flags, exptime, casid); ok {
		this.response = msgStored
	} else {
		if entry == nil {
			this.response = msgNotFound
		} else {
			this.response = msgExists
		}
	}
}

func (this *Server) runGetCmd() {
	key, ok := this.parser.ParseGetCmd()
	if !ok {
		log.Printf("An error occured while parsing «get» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	log.Printf("Parsed «get» command arguments: key=\"%s\"", key)

	value, flags, size, ok := this.cache.Get(string(key[:]))
	if ok {
		log.Printf("Retrieved value=\"%s\" for key=\"%s\"", value, key)
		this.response = fmt.Sprintf("VALUE %s %d %d \r\n%s\r\nEND\r\n", key, flags, size, value)
	} else {
		log.Printf("Cache miss for key=\"%s\"", key)
		this.response = string(strEnd)
	}
}

func (this *Server) runGetsCmd() {
	key, ok := this.parser.ParseGetsCmd()
	if !ok {
		log.Printf("An error occured while parsing «gets» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	log.Printf("Parsed «gets» command arguments: key=\"%s\"", key)

	value, flags, size, casid, ok := this.cache.Gets(string(key[:]))
	if ok {
		log.Printf("Retrieved value=\"%s\" for key=\"%s\"", value, key)
		this.response = fmt.Sprintf("VALUE %s %d %d %d \r\n%s\r\nEND\r\n", key, flags, size, casid, value)
	} else {
		log.Printf("Cache miss for key=\"%s\"", key)
	}
}

func (this *Server) runDeleteCmd() {
	key, ok := this.parser.ParseDeleteCmd()
	if !ok {
		log.Printf("An error occured while parsing «delete» command: cannot parse %s", this.parser.failedToken)
		this.handleInputError(fmt.Sprintf("Cannot parse %s", this.parser.failedToken))
		return
	}

	log.Printf("Parsed «delete» command arguments: key=\"%s\"", key)

	ok = this.cache.Delete(string(key[:]))
	if ok {
		log.Printf("Deleted value for key=\"%s\"", key)
		this.response = msgDeleted
	} else {
		log.Printf("Cannot delete value for key=\"%s\"", key)
		this.response = msgNotFound
	}
}

func (this *Server) handleError() {
	this.response = msgError
}

func (this *Server) handleInputError(errorStr string) {
	this.response = fmt.Sprintf("CLIENT_ERROR %s\r\n", errorStr)
}
