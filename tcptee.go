package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Mapping struct {
	Listen   string
	Primary1 string // App1 - response used for client
	Primary2 string // App2 - response discarded but required
}

func main() {
	var mapsArg string
	var primary2DialTimeout time.Duration

	flag.StringVar(&mapsArg, "maps", "", `Comma-separated mappings: listen=IP:port;primary1=IP:port;primary2=IP:port`)
	flag.DurationVar(&primary2DialTimeout, "primary2-dial-timeout", 3*time.Second, "Timeout for establishing primary2 connection")
	flag.Parse()

	if mapsArg == "" {
		fmt.Fprintln(os.Stderr, "Missing -maps")
		os.Exit(2)
	}

	mappings, err := parseMappings(mapsArg)
	if err != nil {
		log.Fatal(err)
	}

	for _, m := range mappings {
		m := m
		go func() {
			if err := serve(m, primary2DialTimeout); err != nil {
				log.Fatalf("listener %s: %v", m.Listen, err)
			}
		}()
	}

	select {} // forever
}

func parseMappings(s string) ([]Mapping, error) {
	parts := strings.Split(s, ",")
	var out []Mapping
	for _, p := range parts {
		p = strings.TrimSpace(p)
		kv := strings.Split(p, ";")
		m := Mapping{}
		for _, item := range kv {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			x := strings.SplitN(item, "=", 2)
			if len(x) != 2 {
				return nil, fmt.Errorf("bad mapping fragment: %q", item)
			}
			k := strings.TrimSpace(x[0])
			v := strings.TrimSpace(x[1])
			switch k {
			case "listen":
				m.Listen = v
			case "primary1":
				m.Primary1 = v
			case "primary2":
				m.Primary2 = v
			default:
				return nil, fmt.Errorf("unknown key %q in %q", k, item)
			}
		}
		if m.Listen == "" || m.Primary1 == "" || m.Primary2 == "" {
			return nil, fmt.Errorf("incomplete mapping: %q", p)
		}
		out = append(out, m)
	}
	return out, nil
}

func serve(m Mapping, primary2DialTimeout time.Duration) error {
	ln, err := net.Listen("tcp", m.Listen)
	if err != nil {
		return err
	}
	log.Printf("listening %s -> primary1 %s, primary2 %s", m.Listen, m.Primary1, m.Primary2)

	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("accept(%s): %v", m.Listen, err)
			continue
		}
		go handleConn(c, m, primary2DialTimeout)
	}
}

func handleConn(client net.Conn, m Mapping, primary2DialTimeout time.Duration) {
	defer client.Close()

	// Connect to Primary1 (mandatory)
	primary1, err := net.DialTimeout("tcp", m.Primary1, 3*time.Second)
	if err != nil {
		log.Printf("[%s] primary1 dial failed: %v", m.Listen, err)
		return
	}
	defer primary1.Close()

	// Connect to Primary2 (mandatory)
	primary2, err := net.DialTimeout("tcp", m.Primary2, primary2DialTimeout)
	if err != nil {
		log.Printf("[%s] primary2 dial failed: %v", m.Listen, err)
		return
	}
	defer primary2.Close()

	var wg sync.WaitGroup
	wg.Add(3)

	// client -> primary1 AND primary2 (both receive all data)
	go func() {
		defer wg.Done()
		teeCopy(primary1, primary2, client)
		if tcp, ok := primary1.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		if tcp, ok := primary2.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()

	// primary1 -> client (only primary1 response goes to client)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, primary1)
	}()

	// primary2 -> discard (consume and ignore response)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(io.Discard, primary2)
	}()

	wg.Wait()
}

func teeCopy(primary1, primary2 io.Writer, src io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			// Both writes must succeed
			if _, werr := primary1.Write(buf[:n]); werr != nil {
				return
			}
			if _, werr := primary2.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}
