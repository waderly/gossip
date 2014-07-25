package transport

import (
    "github.com/stefankopieczek/gossip/base"
    "github.com/stefankopieczek/gossip/log"
    "github.com/stefankopieczek/gossip/parser"
)

import "net"

type TcpManager struct {
    notifier
    connTable
    laddr *net.TCPAddr
    in *net.TCPListener
}

func NewTcpManager(addr string) (*TcpManager, error) {
    laddr, err := net.ResolveTCPAddr("tcp", addr)
    if err != nil {
        return nil, err
    }

    manager := TcpManager{laddr:laddr}
    manager.connTable.Init()
    return &manager, nil
}

func (mgr *TcpManager) Listen() error {
	var err error = nil
	mgr.in, err = net.ListenTCP("tcp", mgr.laddr)

	if err == nil {
		go mgr.listen()
	}

	return err
}

func (mgr *TcpManager) GetChannel() (c chan base.SipMessage) {
    return mgr.notifier.getChannel()
}

func (mgr *TcpManager) getConn(addr string) (*net.TCPConn, error) {
    var conn *net.TCPConn
    conn = mgr.connTable.GetConn(addr).(*net.TCPConn)
    if conn == nil {
        raddr, err := net.ResolveTCPAddr("tcp", addr)
        if err != nil {
            return nil, err
        }
        conn, err  = net.DialTCP("tcp", mgr.laddr, raddr)
        if err != nil {
            return nil, err
        }
    }

    mgr.connTable.Notify(addr, conn)
    return conn, nil
}

func (mgr *TcpManager) Send(addr string, msg base.SipMessage) error {
    conn, err := mgr.getConn(addr)
    if err != nil {
        return err
    }

    _, err = conn.Write([]byte(msg.String()))
    return err
}

func (mgr *TcpManager) listen() {
    log.Info("Begin listening for TCP on address " + mgr.laddr.String())
	parser := parser.NewMessageParser()
    for {
        conn, err := mgr.in.Accept()
        if err != nil {
            log.Severe("Failed to accept TCP conn on address " + mgr.laddr.String() + "; " + err.Error())
            continue
        }
        go func(c *net.TCPConn) {
            buffer := make([]byte, c_BUFSIZE)
            for {
                num, err := conn.Read(buffer)
                if err != nil {
                    log.Severe("Failed to read from TCP buffer: " + err.Error())
                    continue
                }

                pkt := append([]byte(nil), buffer[:num]...)
                go mgr.handlePacket(pkt, parser)
            }
        }(conn)
	}
}
