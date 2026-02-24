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
	Listen  string
	Primary string
	Shadow  string
}

func main() {
	var mapsArg string
	var shadowTimeout time.Duration
	var shadowDialTimeout time.Duration

	flag.StringVar(&mapsArg, "maps", "", `Comma-separated mappings: listen=IP:port;primary=IP:port;shadow=IP:port`)
	flag.DurationVar(&shadowTimeout, "shadow-timeout", 50*time.Millisecond, "Max time allowed for each shadow write (best-effort)")
	flag.DurationVar(&shadowDialTimeout, "shadow-dial-timeout", 500*time.Millisecond, "Timeout for establishing shadow connection")
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
			if err := serve(m, shadowDialTimeout, shadowTimeout); err != nil {
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
			case "primary":
				m.Primary = v
			case "shadow":
				m.Shadow = v
			default:
				return nil, fmt.Errorf("unknown key %q in %q", k, item)
			}
		}
		if m.Listen == "" || m.Primary == "" || m.Shadow == "" {
			return nil, fmt.Errorf("incomplete mapping: %q", p)
		}
		out = append(out, m)
	}
	return out, nil
}

func serve(m Mapping, shadowDialTimeout, shadowWriteTimeout time.Duration) error {
	ln, err := net.Listen("tcp", m.Listen)
	if err != nil {
		return err
	}
	log.Printf("listening %s -> primary %s, shadow %s", m.Listen, m.Primary, m.Shadow)

	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("accept(%s): %v", m.Listen, err)
			continue
		}
		go handleConn(c, m, shadowDialTimeout, shadowWriteTimeout)
	}
}

func handleConn(client net.Conn, m Mapping, shadowDialTimeout, shadowWriteTimeout time.Duration) {
	defer client.Close()

	primary, err := net.DialTimeout("tcp", m.Primary, 3*time.Second)
	if err != nil {
		log.Printf("[%s] primary dial failed: %v", m.Listen, err)
		return
	}
	defer primary.Close()

	// Shadow is best effort
	var shadow net.Conn
	shadow, err = net.DialTimeout("tcp", m.Shadow, shadowDialTimeout)
	if err != nil {
		shadow = nil
	} else {
		defer shadow.Close()
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// client -> primary (+shadow)
	go func() {
		defer wg.Done()
		teeCopy(primary, shadow, client, shadowWriteTimeout)
		if tcp, ok := primary.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()

	// primary -> client
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, primary)
	}()

	wg.Wait()
}

func teeCopy(primary io.Writer, shadow net.Conn, src io.Reader, shadowWriteTimeout time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			// Primary write must succeed
			if _, werr := primary.Write(buf[:n]); werr != nil {
				return
			}
			// Shadow write best-effort
			if shadow != nil {
				_ = shadow.SetWriteDeadline(time.Now().Add(shadowWriteTimeout))
				_, _ = shadow.Write(buf[:n])
				_ = shadow.SetWriteDeadline(time.Time{})
			}
		}
		if rerr != nil {
			return
		}
	}
}
