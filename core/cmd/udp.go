package cmd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"syscall"

	"github.com/vkcom/kittenhouse/core/inmem"
)

var (
	errPacketTooShort = errors.New("Packet too short")
	errInvalidCRC     = errors.New("Invalid CRC32")
	errMissingNulByte = errors.New("Missing nul byte")
)

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

	nulPos := bytes.IndexByte(buf, 0)
	if nulPos < 0 {
		return "", nil, 0, errMissingNulByte
	}

	tableOffset := 0
	flags = 0

	if buf[0] < 32 {
		tableOffset = 1
		flags = buf[0]
	}

	table = string(buf[tableOffset:nulPos])
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
		n, _, err := conn.ReadFromUDP(buf)
		if err == syscall.EINVAL {
			log.Fatalf("Could not read from UDP socket: %s", err.Error())
		} else if err != nil {
			log.Printf("Could not read from UDP: %s", err.Error())
			continue
		}

		table, data, flags, err := parseUDPPacket(buf[0:n])
		if err != nil {
			log.Printf("Got bad packet from UDP: %s", err.Error())
			continue
		}

		inmem.Write(table, data, flags&FlagRowBinary == FlagRowBinary)
	}
}
