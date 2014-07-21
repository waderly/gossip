package transport

import (
    "github.com/stefankopieczek/gossip/base"
    "github.com/stefankopieczek/gossip/log"
    "github.com/stefankopieczek/gossip/parser"
)

import (
    "net"
)

const c_BUFSIZE int = 65507

type UdpTransportManager struct {
    notifier
	address *net.UDPAddr
    in      *net.UDPConn
}

func NewUdpTransportManager(address *net.UDPAddr) (*UdpTransportManager) {
	manager := UdpTransportManager{notifier{}, address, nil}
	return &manager
}

func (transport *UdpTransportManager) Listen() error {
	var err error = nil
	transport.in, err = net.ListenUDP("udp", transport.address)

	if err == nil {
		go transport.listen()
	}

	return err
}

func (transport *UdpTransportManager) GetChannel() (c chan base.SipMessage) {
    return transport.notifier.getChannel()
}

func (transport *UdpTransportManager) Send(addr string, msg base.SipMessage) error {
    raddr, err := net.ResolveUDPAddr("udp", addr)
    if err != nil {
        return err
    }

    var conn *net.UDPConn
    conn, err = net.DialUDP("udp", transport.address, raddr)
    if err != nil {
        return err
    }
    defer conn.Close()

    _, err = conn.Write([]byte(msg.String()))
    return err
}

func (transport *UdpTransportManager) listen() {
    log.Info("Begin listening for UDP on address " + transport.address.String())
	parser := parser.NewMessageParser()
	buffer := make([]byte, c_BUFSIZE)
	for {
		num, _, err := transport.in.ReadFromUDP(buffer)
		if err != nil {
            log.Severe("Failed to read from UDP buffer: " + err.Error())
            continue
		}

		pkt := append([]byte(nil), buffer[:num]...)
		go transport.handlePacket(pkt, parser)
	}
}

func (transport *UdpTransportManager) handlePacket(pkt []byte, parser parser.MessageParser) {
	message, err := parser.ParseMessage(pkt)

	if err != nil {
        log.Debug("Failed to parse message: " + err.Error())
		return
	}

    transport.notifier.notifyAll(message)
}

func (transport *UdpTransportManager) Stop() {
    transport.in.Close()
}
