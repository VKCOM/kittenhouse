package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

func TestTableNameCheck(t *testing.T) {
	if !tableNameIsOK([]byte("Omg_1ol (a,b,c)")) {
		t.Errorf("Table name 'Omg_1ol (a,b,c)' must be valid")
	}

	if tableNameIsOK([]byte{'h', 'e', 'l', 1, 2, 3, 4, 'o'}) {
		t.Errorf(`Table name 'hel\01\02\03\04o' must not be valid`)
	}
}

type faults struct {
	wrongCrc bool
}

func writePacket(t *testing.T, c *net.UDPConn, table string, contents string, f faults) {
	buf := make([]byte, 0, len(table)+len(contents)+6)
	buf = append(buf, FlagRowBinary) // flags
	buf = append(buf, table...)
	buf = append(buf, 0)
	buf = append(buf, contents...)

	tmpBuf := make([]byte, 4)

	if !f.wrongCrc {
		binary.LittleEndian.PutUint32(tmpBuf, crc32.Update(0, crc32.IEEETable, buf))
	}

	buf = append(buf, tmpBuf...)

	_, err := c.Write(buf)
	if err != nil {
		t.Fatalf("Could not write packet: %s", err.Error())
	}
}

// This test is to only launched manually
func TestUDPFunctional(t *testing.T) {
	if os.Getenv("RUN_FUNCTIONAL") == "" {
		return
	}

	argv.host = "127.0.0.1"
	argv.port = 1234

	go listenUDP()

	time.Sleep(time.Millisecond * 100)

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", argv.host, argv.port))
	if err != nil {
		t.Fatalf("Could not resolve udp addr: %s", err.Error())
	}

	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatalf("Could not dial udp addr %s", addr)
	}
	defer c.Close()

	log.Printf("Sending good packet")
	writePacket(t, c, "omglol", "hello world", faults{})
	log.Printf("/Sending good packet")

	log.Printf("Sending bad CRC")
	writePacket(t, c, "omglol2", "hello world2", faults{wrongCrc: true})
	time.Sleep(time.Millisecond * 50)
	log.Printf("/Sending bad CRC")

	time.Sleep(time.Second)

	log.Printf("Sending bad table")
	writePacket(t, c, "omglol $@!T$#", "hello world2", faults{})
	time.Sleep(time.Millisecond * 50)
	log.Printf("/Sending bad table")

	log.Printf("Sending good packet")
	writePacket(t, c, "omglol4", "hello world4", faults{})
	log.Printf("/Sending good packet")

	time.Sleep(time.Second)
}
