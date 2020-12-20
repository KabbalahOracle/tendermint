package p2p

// scheduler does QoS scheduling for Envelopes, enqueueing and dequeueing
// according to some policy. Schedulers are used at contention points, i.e.:
//
// - Receiving inbound messages for a single channel from all peers.
// - Sending outbound messages to a single peer from all channels.
//
// As such, enqueue() is a blocking method call that is expected to be called
// concurrently, and dequeue() returns a channel that is expected to be consumed
// serially.
type scheduler interface {
	// enqueue enqueues a message envelope.
	enqueue(Envelope)

	// dequeue returns an envelope channel ordered according to some policy.
	dequeue() <-chan Envelope
}

// fifoScheduler is a simple lossless scheduler that passes messages through in
// the order they were received. If the channel is full, enqueue() will block.
type fifoScheduler struct {
	ch chan Envelope
}

func newFIFOScheduler(ch chan Envelope) *fifoScheduler {
	return &fifoScheduler{ch: ch}
}

func (s *fifoScheduler) dequeue() <-chan Envelope {
	return s.ch
}

func (s *fifoScheduler) enqueue(envelope Envelope) {
	s.ch <- envelope
}
