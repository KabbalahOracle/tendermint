package p2p_test

import (
	"testing"
	"time"

	gogotypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
)

type TestMessage = gogotypes.StringValue

func echoReactor(channel *p2p.Channel) {
	for {
		select {
		case envelope := <-channel.In():
			channel.Out() <- p2p.Envelope{
				To:      envelope.From,
				Message: &TestMessage{Value: envelope.Message.(*TestMessage).Value},
			}
		case <-channel.Done():
			return
		}
	}
}

func TestRouter(t *testing.T) {
	logger := log.TestingLogger()
	network := p2p.NewMemoryNetwork(logger)
	transport := network.GenerateTransport()
	chID := p2p.ChannelID(1)

	peers := []p2p.PeerAddress{}
	for i := 0; i < 3; i++ {
		peerTransport := network.GenerateTransport()
		peerRouter := p2p.NewRouter(logger.With("peerID", i), map[p2p.Protocol]p2p.Transport{
			p2p.MemoryProtocol: peerTransport,
		}, nil)
		peers = append(peers, peerTransport.Endpoints()[0].PeerAddress())

		channel, err := peerRouter.OpenChannel(chID, &TestMessage{})
		require.NoError(t, err)
		defer channel.Close()
		go echoReactor(channel)

		err = peerRouter.Start()
		require.NoError(t, err)
		defer func() { require.NoError(t, peerRouter.Stop()) }()
	}

	router := p2p.NewRouter(logger, map[p2p.Protocol]p2p.Transport{
		p2p.MemoryProtocol: transport,
	}, peers)
	channel, err := router.OpenChannel(chID, &TestMessage{})
	require.NoError(t, err)

	err = router.Start()
	require.NoError(t, err)
	defer func() { require.NoError(t, router.Stop()) }()

	// FIXME: Should use peer updates to wait for peers to become available.
	time.Sleep(time.Second)
	for _, peer := range peers {
		channel.Out() <- p2p.Envelope{To: peer.PeerID(), Message: &TestMessage{Value: "hi!"}}
		assert.Equal(t, p2p.Envelope{
			From:    peer.PeerID(),
			Message: &TestMessage{Value: "hi!"},
		}, <-channel.In())
	}

	time.Sleep(3 * time.Second)
}
