package transport

import (
    "github.com/stefankopieczek/gossip/base"
    "github.com/stefankopieczek/gossip/log"
)

import (
    "fmt"
    "sync"
)

const c_LISTENER_QUEUE_SIZE int = 1000

/**
  Temporary implementation of a transport layer for SIP messages using UDP.
  This will be heavily revised, and should not be relied upon.
*/

type Manager interface {
	Listen()
	Send(message base.SipMessage)
	GetChannel() chan base.SipMessage
	Stop()
}

type notifier struct {
	listeners map[listener]bool
	listenerLock sync.Mutex
}

func (n *notifier) register(l listener) {
    if n.listeners == nil {
        n.listeners = make(map[listener]bool)
    }
	n.listenerLock.Lock()
	n.listeners[l] = true
	n.listenerLock.Unlock()
}

func (n *notifier) getChannel() (l listener) {
    c := make(chan base.SipMessage, c_LISTENER_QUEUE_SIZE)
    n.register(c)
    return c
}

func (n *notifier) notifyAll(msg base.SipMessage) {
	// Dispatch the message to all registered listeners.
	// If the listener is a closed channel, remove it from the list.
	deadListeners := make([]chan base.SipMessage, 0)
	n.listenerLock.Lock()
    log.Debug(fmt.Sprintf("Notify %d listeners of message", len(n.listeners)))
	for listener := range n.listeners {
		sent := listener.notify(msg)
		if !sent {
			deadListeners = append(deadListeners, listener)
		}
	}
	for _, deadListener := range deadListeners {
        log.Debug(fmt.Sprintf("Expiring listener %#v", deadListener))
		delete(n.listeners, deadListener)
	}
	n.listenerLock.Unlock()
}

type listener chan base.SipMessage

// notify tries to send a message to the listener.
// If the underlying channel has been closed by the receiver, return 'false';
// otherwise, return true.
func (c listener) notify(message base.SipMessage) (ok bool) {
	defer func() { recover() }()
	c <- message
	return true
}
