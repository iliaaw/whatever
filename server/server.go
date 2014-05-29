package main

import (
	"flag"
	"github.com/ilyakhokhryakov/whatever"
)

func main() {
	verbose := flag.Bool("v", false, "enable verbose mode")
	addr := flag.String("a", "0.0.0.0:9336", "address to listen")
	maxLength := flag.Int("m", 4*1024*1024, "max cache size")
	flag.Parse()

	server := whatever.NewServer(*addr, *verbose, *maxLength)
	server.Start()
}
