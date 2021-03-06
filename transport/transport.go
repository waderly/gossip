package transport

import (
    "github.com/stefankopieczek/gossip/base"
    "github.com/stefankopieczek/gossip/parser"
)

import (
    "fmt"
    "net"
    "sync"
)

/**
  Temporary implementation of a transport layer for SIP messages using UDP.
  This will be heavily revised, and should not be relied upon.
*/

type SipTransportManager interface {
	Start()
	Send(message *base.SipMessage)
	GetChannel()
	Stop()
}

type listener chan base.SipMessage

// notify tries to send a message to the listener.
// If the underlying channel has been closed by the receiver, return 'false';
// otherwise, return true.
func (l listener) notify(message base.SipMessage) (ok bool) {
	defer func() { recover() }()
	l <- message
	return true
}

type UdpTransportManager struct {
	address      *net.UDPAddr
	conn         *net.UDPConn
	listeners    map[listener]bool
	listenerLock sync.Mutex
}

func NewUdpTransportManager(address *net.UDPAddr) (*UdpTransportManager, error) {
	listeners := make(map[listener]bool, 0)
	var listenerLock sync.Mutex
	manager := UdpTransportManager{address, nil, listeners, listenerLock}
	return &manager, nil
}

func (transport *UdpTransportManager) Start() error {
	var err error = nil
	transport.conn, err = net.ListenUDP("udp", transport.address)

	if err == nil {
		go transport.listen()
	}

	return err
}

func (transport *UdpTransportManager) GetChannel() (c chan base.SipMessage) {
	c = make(chan base.SipMessage)

	transport.listenerLock.Lock()
	transport.listeners[c] = true
	transport.listenerLock.Unlock()

	return c
}

func (transport *UdpTransportManager) listen() {
	fmt.Printf("Listening.\n")
	parser := parser.NewMessageParser()
	buffer := make([]byte, 65507)
	for {
		num, _, err := transport.conn.ReadFromUDP(buffer) // TODO: Do this properly.
		if err != nil {
			panic(err)
		}

		pkt := append([]byte(nil), buffer[:num]...)
		go transport.handlePacket(pkt, parser)
	}
}

func (transport *UdpTransportManager) handlePacket(pkt []byte, parser parser.MessageParser) {
	message, err := parser.ParseMessage(pkt)

	// TODO: Test hack
	if err != nil {
		fmt.Printf("Error:\n%s\n\n", err.Error())
		return
	}

	// Dispatch the message to all registered listeners.
	// If the listener is a closed channel, remove it from the list.
	deadListeners := make([]chan base.SipMessage, 0)
	transport.listenerLock.Lock()
	for listener := range transport.listeners {
		sent := listener.notify(message)
		if !sent {
			deadListeners = append(deadListeners, listener)
		}
	}
	for _, deadListener := range deadListeners {
		delete(transport.listeners, deadListener)
	}
	transport.listenerLock.Unlock()
}
