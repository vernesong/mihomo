package mkcp

import "encoding/binary"

type command byte

const (
	commandACK       command = 0
	commandData      command = 1
	commandTerminate command = 2
	commandPing      command = 3
)

type segmentOption byte

const (
	segmentOptionClose segmentOption = 1
)

const dataSegmentOverhead = 18

type segment interface {
	conversation() uint16
	command() command
	byteSize() int
	serialize([]byte)
}

type dataSegment struct {
	conv        uint16
	option      segmentOption
	timestamp   uint32
	number      uint32
	sendingNext uint32
	payload     []byte
	timeout     uint32
	transmit    uint32
}

func (s *dataSegment) conversation() uint16 { return s.conv }
func (*dataSegment) command() command       { return commandData }
func (s *dataSegment) byteSize() int        { return dataSegmentOverhead + len(s.payload) }

func (s *dataSegment) serialize(b []byte) {
	binary.BigEndian.PutUint16(b, s.conv)
	b[2] = byte(commandData)
	b[3] = byte(s.option)
	binary.BigEndian.PutUint32(b[4:], s.timestamp)
	binary.BigEndian.PutUint32(b[8:], s.number)
	binary.BigEndian.PutUint32(b[12:], s.sendingNext)
	binary.BigEndian.PutUint16(b[16:], uint16(len(s.payload)))
	copy(b[18:], s.payload)
}

type ackSegment struct {
	conv            uint16
	option          segmentOption
	receivingWindow uint32
	receivingNext   uint32
	timestamp       uint32
	numberList      []uint32
}

func (s *ackSegment) conversation() uint16 { return s.conv }
func (*ackSegment) command() command       { return commandACK }
func (s *ackSegment) byteSize() int        { return 17 + len(s.numberList)*4 }

func (s *ackSegment) serialize(b []byte) {
	binary.BigEndian.PutUint16(b, s.conv)
	b[2] = byte(commandACK)
	b[3] = byte(s.option)
	binary.BigEndian.PutUint32(b[4:], s.receivingWindow)
	binary.BigEndian.PutUint32(b[8:], s.receivingNext)
	binary.BigEndian.PutUint32(b[12:], s.timestamp)
	b[16] = byte(len(s.numberList))
	n := 17
	for _, number := range s.numberList {
		binary.BigEndian.PutUint32(b[n:], number)
		n += 4
	}
}

type cmdOnlySegment struct {
	conv          uint16
	cmd           command
	option        segmentOption
	sendingNext   uint32
	receivingNext uint32
	peerRTO       uint32
}

func (s *cmdOnlySegment) conversation() uint16 { return s.conv }
func (s *cmdOnlySegment) command() command     { return s.cmd }
func (*cmdOnlySegment) byteSize() int          { return 16 }

func (s *cmdOnlySegment) serialize(b []byte) {
	binary.BigEndian.PutUint16(b, s.conv)
	b[2] = byte(s.cmd)
	b[3] = byte(s.option)
	binary.BigEndian.PutUint32(b[4:], s.sendingNext)
	binary.BigEndian.PutUint32(b[8:], s.receivingNext)
	binary.BigEndian.PutUint32(b[12:], s.peerRTO)
}

func readSegment(b []byte) (segment, []byte) {
	if len(b) < 4 {
		return nil, nil
	}
	conv := binary.BigEndian.Uint16(b)
	cmd := command(b[2])
	opt := segmentOption(b[3])
	b = b[4:]
	switch cmd {
	case commandData:
		if len(b) < 14 {
			return nil, nil
		}
		dataLen := int(binary.BigEndian.Uint16(b[12:]))
		if len(b) < 14+dataLen {
			return nil, nil
		}
		seg := &dataSegment{
			conv:        conv,
			option:      opt,
			timestamp:   binary.BigEndian.Uint32(b),
			number:      binary.BigEndian.Uint32(b[4:]),
			sendingNext: binary.BigEndian.Uint32(b[8:]),
			payload:     append([]byte(nil), b[14:14+dataLen]...),
		}
		return seg, b[14+dataLen:]
	case commandACK:
		if len(b) < 13 {
			return nil, nil
		}
		count := int(b[12])
		if len(b) < 13+count*4 {
			return nil, nil
		}
		seg := &ackSegment{
			conv:            conv,
			option:          opt,
			receivingWindow: binary.BigEndian.Uint32(b),
			receivingNext:   binary.BigEndian.Uint32(b[4:]),
			timestamp:       binary.BigEndian.Uint32(b[8:]),
			numberList:      make([]uint32, count),
		}
		p := b[13:]
		for i := 0; i < count; i++ {
			seg.numberList[i] = binary.BigEndian.Uint32(p)
			p = p[4:]
		}
		return seg, p
	default:
		if len(b) < 12 {
			return nil, nil
		}
		seg := &cmdOnlySegment{
			conv:          conv,
			cmd:           cmd,
			option:        opt,
			sendingNext:   binary.BigEndian.Uint32(b),
			receivingNext: binary.BigEndian.Uint32(b[4:]),
			peerRTO:       binary.BigEndian.Uint32(b[8:]),
		}
		return seg, b[12:]
	}
}
