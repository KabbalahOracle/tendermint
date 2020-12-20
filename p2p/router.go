package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/libs/service"
)

// Router manages peer connections and routes messages between peers and
// channels.
type Router struct {
	*service.BaseService
	logger     log.Logger
	transports map[Protocol]Transport
	store      *peerStore

	chClose    chan struct{}
	wgChannels sync.WaitGroup
	wgPeers    sync.WaitGroup

	// FIXME: should use a finer-grained mutex, e.g. one per resource type.
	// FIXME: consider using sync.Map for channels and peers.
	mtx         sync.RWMutex
	channels    map[ChannelID]*Channel
	peers       map[string]Connection
	peerUpdates map[*PeerUpdatesCh]*PeerUpdatesCh // keyed by struct identity (address)
}

// NewRouter creates a new Router.
func NewRouter(logger log.Logger, transports map[Protocol]Transport, peers []PeerAddress) *Router {
	store := newPeerStore(logger)
	for _, address := range peers {
		if err := store.Add(address); err != nil {
			logger.Error("failed to add peer", "address", address, "err", err)
		}
	}
	router := &Router{
		logger:      logger,
		transports:  transports,
		store:       store,
		chClose:     make(chan struct{}),
		channels:    map[ChannelID]*Channel{},
		peers:       map[string]Connection{},
		peerUpdates: map[*PeerUpdatesCh]*PeerUpdatesCh{},
	}
	router.BaseService = service.NewBaseService(logger, "router", router)
	return router
}

// OpenChannel opens a new channel for the given message type.
func (r *Router) OpenChannel(id ChannelID, messageType proto.Message) (*Channel, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if _, ok := r.channels[id]; ok {
		return nil, fmt.Errorf("channel %v already in use", id)
	}

	channel := NewChannel(id, messageType, make(chan Envelope), make(chan Envelope), nil) // FIXME: handle PeerError
	r.channels[id] = channel
	r.wgChannels.Add(1)
	go func() {
		defer func() {
			r.mtx.Lock()
			delete(r.channels, id)
			r.mtx.Unlock()
			r.wgChannels.Done()
		}()
		r.receiveChannel(channel)
	}()

	return channel, nil
}

// SubscribePeerUpdates creates a new peer updates subscription.
func (r *Router) SubscribePeerUpdates() (*PeerUpdatesCh, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	peerUpdates := NewPeerUpdates()
	r.peerUpdates[peerUpdates] = peerUpdates
	go func() {
		select {
		case <-peerUpdates.Done():
			r.mtx.Lock()
			defer r.mtx.Unlock()
			delete(r.peerUpdates, peerUpdates)
		case <-r.chClose:
		}
	}()
	return peerUpdates, nil
}

// acceptPeers accepts inbound connections from peers on the given transport.
func (r *Router) acceptPeers(transport Transport) {
	for {
		select {
		case <-r.chClose:
			return
		default:
		}

		conn, err := transport.Accept(context.Background())
		switch err {
		case ErrTransportClosed{}:
			return
		case io.EOF:
			return
		case nil:
		default:
			r.logger.Error("failed to accept connection", "err", err)
			return
		}

		peerID, err := conn.NodeInfo().DefaultNodeID.ToPeerID()
		if err != nil {
			r.logger.Error("invalid peer ID", "peer", conn.NodeInfo().DefaultNodeID)
			_ = conn.Close()
			continue
		}
		peer := r.store.Claim(peerID)
		if peer == nil {
			r.logger.Error("already connected to peer, rejecting connection", "peer", peerID)
			_ = conn.Close()
			continue
		}

		r.wgPeers.Add(1)
		go func() {
			defer r.wgPeers.Done()
			defer r.store.Return(peer.ID)
			defer conn.Close()
			err = r.receivePeer(peer, conn)
			if err == io.EOF || err == nil {
				r.logger.Info("peer disconnected", "peer", peer.ID)
			} else if err != nil {
				r.logger.Error("peer failure", "peer", peer.ID, "err", err)
			}
		}()
	}
}

// dialPeers maintains outbound connections to peers.
func (r *Router) dialPeers() {
	for {
		select {
		case <-r.chClose:
			return
		default:
		}

		peer := r.store.Dispense()
		if peer == nil {
			// no dispensed peers, sleep for a while
			r.logger.Info("no eligible peers, sleeping")
			time.Sleep(time.Second)
			continue
		}

		r.wgPeers.Add(1)
		go func() {
			defer r.wgPeers.Done()
			defer r.store.Return(peer.ID)
			conn, err := r.dialPeer(peer)
			if err != nil {
				r.logger.Error("failed to dial peer, will retry", "peer", peer.ID)
				return
			}
			defer conn.Close()
			err = r.receivePeer(peer, conn)
			if err != nil {
				r.logger.Error("peer failure", "peer", peer.ID, "err", err)
			}
		}()
	}
}

// dialPeer attempts to connect to a peer.
func (r *Router) dialPeer(peer *sPeer) (Connection, error) {
	ctx := context.Background()

	r.logger.Info("resolving peer endpoints", "peer", peer.ID)
	endpoints := []Endpoint{}
	for _, address := range peer.Addresses {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		e, err := address.Resolve(ctx)
		if err != nil {
			r.logger.Error("failed to resolve address", "address", address, "err", err)
			continue
		}
		endpoints = append(endpoints, e...)
	}
	if len(endpoints) == 0 {
		return nil, errors.New("unable to resolve any network endpoints")
	}

	for _, endpoint := range endpoints {
		t, ok := r.transports[endpoint.Protocol]
		if !ok {
			r.logger.Error("no transport found for protocol", "protocol", endpoint.Protocol)
			continue
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		conn, err := t.Dial(ctx, endpoint)
		if err != nil {
			r.logger.Error("failed to dial endpoint", "endpoint", endpoint)
		} else {
			r.logger.Info("connected to peer", "peer", peer.ID, "endpoint", endpoint)
			return conn, nil
		}
	}
	return nil, errors.New("failed to connect to peer")
}

// receivePeer receives inbound messages from a peer and passes them
// on to the channel.
func (r *Router) receivePeer(peer *sPeer, conn Connection) error {
	for {
		chID, bz, err := conn.ReceiveMessage()
		if err != nil {
			return err
		}

		// FIXME: Avoid mutex here.
		r.mtx.RLock()
		channel, ok := r.channels[ChannelID(chID)]
		r.mtx.RUnlock()
		if !ok {
			r.logger.Error("dropping message for unknown channel", "peer", peer.ID, "channel", chID)
			continue
		}

		msg := proto.Clone(channel.messageType)
		msg.Reset() // FIXME: This should be done once, during channel construction.
		if err := proto.Unmarshal(bz, msg); err != nil {
			r.logger.Error("message decoding failed, dropping message", "peer", peer.ID, "err", err)
			continue
		}

		select {
		case channel.inCh <- Envelope{From: peer.ID, Message: msg}:
			r.logger.Debug("received message", "peer", peer.ID, "message", msg)
		case <-channel.doneCh:
			r.logger.Error("channel closed, dropping message", "peer", peer.ID, "channel", chID)
		case <-r.chClose:
			return nil
		}
	}
}

// receiveChannel receives outbound messages from a channel and sends them to the
// appropriate peer.
func (r *Router) receiveChannel(channel *Channel) {
	for {
		select {
		case envelope, ok := <-channel.outCh:
			if !ok {
				return
			}
			r.mtx.Lock()
			conn, ok := r.peers[envelope.To.String()]
			r.mtx.Unlock()
			if !ok {
				r.logger.Error("dropping message for non-connected peer", "peer", envelope.To.String())
				continue
			}

			bz, err := proto.Marshal(envelope.Message)
			if err != nil {
				r.logger.Error("failed to marshal message", "peer", envelope.To.String(), "err", err)
				continue
			}

			// FIXME: We need a sender goroutine per peer.
			// FIXME: Should take ChannelID.
			_, err = conn.SendMessage(byte(channel.id), bz)
			if err != nil {
				r.logger.Error("failed to send to peer", "peer", envelope.To.String(), "err", err)
				_ = conn.Close()
				continue
			}
		case <-channel.doneCh:
			return
		case <-r.chClose:
			return
		}
	}
}

// OnStart implements service.Service.
func (r *Router) OnStart() error {
	go r.dialPeers()
	for _, transport := range r.transports {
		go r.acceptPeers(transport)
	}
	return nil
}

// OnStop implements service.Service.
func (r *Router) OnStop() {
	close(r.chClose)
	r.wgPeers.Wait()
}
