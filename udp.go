package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"strconv"
	"syscall"
	"time"

	"github.com/vkcom/kittenhouse/inmem"
)

var (
	errPacketTooShort = errors.New("Packet too short")
	errInvalidCRC     = errors.New("Invalid CRC32")
	errMissingNulByte = errors.New("Missing nul byte")
	errBadTableName   = errors.New("Bad table name: corrupt packet?")

	badBacketRateLimiter = time.Tick(time.Second)
)

func tableNameIsOK(table []byte) bool {
	for _, c := range table {
		if !(c >= 'A' && c <= 'Z' ||
			c >= 'a' && c <= 'z' ||
			c >= '0' && c <= '9' ||
			c == '(' || c == ')' || c == ' ' || c == '_' || c == ',') {
			return false
		}
	}

	return true
}

func logBadPacket(buf []byte) {
	select {
	case <-badBacketRateLimiter:
	default:
		return
	}

	log.Printf("Received bad UDP packet (bad table name): %x (%s)", buf, strconv.QuoteToASCII(string(buf)))
}

// parseUDPPacket parses UDP packet in the following format:
// | table_name | \0 | data | crc32 (4 byte) |
func parseUDPPacket(buf []byte) (table string, data []byte, flags byte, err error) {
	if len(buf) < 5 {
		return "", nil, 0, errPacketTooShort
	}

	crcBytes := buf[len(buf)-4:]
	buf = buf[0 : len(buf)-4]

	expectedCrc := binary.LittleEndian.Uint32(crcBytes)

	if crc32.Update(0, crc32.IEEETable, buf) != expectedCrc {
		return "", nil, 0, errInvalidCRC
	}

	flags = 0
	if buf[0] < 32 {
		flags = buf[0]
		buf = buf[1:]
	}

	nulPos := bytes.IndexByte(buf, 0)
	if nulPos < 0 {
		return "", nil, 0, errMissingNulByte
	}

	tableBytes := buf[:nulPos]

	if !tableNameIsOK(tableBytes) {
		return "", nil, 0, errBadTableName
	}

	table = string(tableBytes)
	data = buf[nulPos+1:]

	return table, data, flags, nil
}

func listenUDP() {
	buf := make([]byte, maxUDPPacketSize)

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", argv.host, argv.port))
	if err != nil {
		log.Fatalf("Could not resolve udp addr: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Could not listen udp addr: %s", err.Error())
	}
	defer conn.Close()

	log.Printf("Listening %s:%d (UDP)", argv.host, argv.port)

	for {
		n, err := conn.Read(buf)
		if err == syscall.EINVAL {
			log.Fatalf("Could not read from UDP socket: %s", err.Error())
		} else if err != nil {
			log.Printf("Could not read from UDP: %s", err.Error())
			continue
		}

		table, data, flags, err := parseUDPPacket(buf[0:n])
		if err != nil {
			if err == errBadTableName {
				logBadPacket(buf[0:n])
			} else {
				log.Printf("Got bad packet from UDP: %s", err.Error())
			}
			continue
		}

		inmem.Write(table, data, flags&FlagRowBinary == FlagRowBinary)
	}
}
