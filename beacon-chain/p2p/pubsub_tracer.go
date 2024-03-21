package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	gk "github.com/dennis-tra/go-kinesis"
	"github.com/google/uuid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prometheus/client_golang/prometheus"
)

type traceEvent struct {
	Type      string
	PeerID    peer.ID
	Timestamp time.Time
	Payload   any `json:"Data"` // cannot use field "Data" because of gk.Record method
}

var _ gk.Record = (*traceEvent)(nil)

func (t *traceEvent) PartitionKey() string {
	u, err := uuid.NewUUID()
	if err != nil {
		return t.PeerID.String()
	}
	return u.String()
}

func (t *traceEvent) ExplicitHashKey() *string {
	return nil
}

func (t *traceEvent) Data() []byte {
	data, err := json.Marshal(t)
	if err != nil {
		log.WithError(err).Warn("Failed to marshal trace event")
		return nil
	}
	return data
}

var _ = pubsub.RawTracer(gossipTracer{})

// Initializes the values for the pubsub rpc action.
type action int

const (
	recv action = iota
	send
	drop
)

// This tracer is used to implement metrics collection for messages received
// and broadcasted through gossipsub.
type gossipTracer struct {
	host     host.Host
	producer *gk.Producer
}

// AddPeer .
func (g gossipTracer) AddPeer(p peer.ID, proto protocol.ID) {
	g.flushTrace(pubsubpb.TraceEvent_ADD_PEER.String(), map[string]any{
		"PeerID":   p,
		"Protocol": proto,
	})
}

// RemovePeer .
func (g gossipTracer) RemovePeer(p peer.ID) {
	g.flushTrace(pubsubpb.TraceEvent_REMOVE_PEER.String(), map[string]any{
		"PeerID": p,
	})
	// no-op
}

// Join .
func (g gossipTracer) Join(topic string) {
	pubsubTopicsActive.WithLabelValues(topic).Set(1)
	g.flushTrace(pubsubpb.TraceEvent_JOIN.String(), map[string]any{
		"Topic": topic,
	})
}

// Leave .
func (g gossipTracer) Leave(topic string) {
	pubsubTopicsActive.WithLabelValues(topic).Set(0)
	g.flushTrace(pubsubpb.TraceEvent_LEAVE.String(), map[string]any{
		"Topic": topic,
	})
}

// Graft .
func (g gossipTracer) Graft(p peer.ID, topic string) {
	pubsubTopicsGraft.WithLabelValues(topic).Inc()
	g.flushTrace(pubsubpb.TraceEvent_GRAFT.String(), map[string]any{
		"PeerID": p,
		"Topic":  topic,
	})
}

// Prune .
func (g gossipTracer) Prune(p peer.ID, topic string) {
	pubsubTopicsPrune.WithLabelValues(topic).Inc()
	g.flushTrace(pubsubpb.TraceEvent_PRUNE.String(), map[string]any{
		"PeerID": p,
		"Topic":  topic,
	})
}

// ValidateMessage .
func (g gossipTracer) ValidateMessage(msg *pubsub.Message) {
	pubsubMessageValidate.WithLabelValues(*msg.Topic).Inc()
	g.flushTrace("VALIDATE_MESSAGE", map[string]any{
		"PeerID":   msg.ReceivedFrom,
		"Topic":    msg.GetTopic(),
		"MsgID":    hex.EncodeToString([]byte(msg.ID)),
		"Local":    msg.Local,
		"MsgBytes": msg.Size(),
		"SeqNo":    hex.EncodeToString(msg.GetSeqno()),
	})
}

// DeliverMessage .
func (g gossipTracer) DeliverMessage(msg *pubsub.Message) {
	pubsubMessageDeliver.WithLabelValues(*msg.Topic).Inc()
	g.flushTrace(pubsubpb.TraceEvent_DELIVER_MESSAGE.String(), map[string]any{
		"PeerID":   msg.ReceivedFrom,
		"Topic":    msg.GetTopic(),
		"MsgID":    hex.EncodeToString([]byte(msg.ID)),
		"Local":    msg.Local,
		"MsgBytes": msg.Size(),
		"Seq":      hex.EncodeToString(msg.GetSeqno()),
	})
}

// RejectMessage .
func (g gossipTracer) RejectMessage(msg *pubsub.Message, reason string) {
	pubsubMessageReject.WithLabelValues(*msg.Topic, reason).Inc()
	g.flushTrace(pubsubpb.TraceEvent_REJECT_MESSAGE.String(), map[string]any{
		"PeerID":   msg.ReceivedFrom,
		"Topic":    msg.GetTopic(),
		"MsgID":    hex.EncodeToString([]byte(msg.ID)),
		"Reason":   reason,
		"Local":    msg.Local,
		"MsgBytes": msg.Size(),
		"Seq":      hex.EncodeToString(msg.GetSeqno()),
	})
}

// DuplicateMessage .
func (g gossipTracer) DuplicateMessage(msg *pubsub.Message) {
	pubsubMessageDuplicate.WithLabelValues(*msg.Topic).Inc()
	g.flushTrace(pubsubpb.TraceEvent_DUPLICATE_MESSAGE.String(), map[string]any{
		"PeerID":   msg.ReceivedFrom,
		"Topic":    msg.GetTopic(),
		"MsgID":    hex.EncodeToString([]byte(msg.ID)),
		"Local":    msg.Local,
		"MsgBytes": msg.Size(),
		"Seq":      hex.EncodeToString(msg.GetSeqno()),
	})
}

// UndeliverableMessage .
func (g gossipTracer) UndeliverableMessage(msg *pubsub.Message) {
	pubsubMessageUndeliverable.WithLabelValues(*msg.Topic).Inc()
	g.flushTrace("UNDELIVERABLE_MESSAGE", map[string]any{
		"PeerID": msg.ReceivedFrom,
		"Topic":  msg.GetTopic(),
		"MsgID":  hex.EncodeToString([]byte(msg.ID)),
		"Local":  msg.Local,
	})
}

// ThrottlePeer .
func (g gossipTracer) ThrottlePeer(p peer.ID) {
	agent := agentFromPid(p, g.host.Peerstore())
	pubsubPeerThrottle.WithLabelValues(agent).Inc()
	g.flushTrace("THROTTLE_PEER", map[string]any{
		"PeerID": p,
	})
}

// RecvRPC .
func (g gossipTracer) RecvRPC(rpc *pubsub.RPC) {
	g.setMetricFromRPC(recv, pubsubRPCSubRecv, pubsubRPCPubRecv, pubsubRPCRecv, rpc)
}

// SendRPC .
func (g gossipTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	g.setMetricFromRPC(send, pubsubRPCSubSent, pubsubRPCPubSent, pubsubRPCSent, rpc)
}

// DropRPC .
func (g gossipTracer) DropRPC(rpc *pubsub.RPC, p peer.ID) {
	g.setMetricFromRPC(drop, pubsubRPCSubDrop, pubsubRPCPubDrop, pubsubRPCDrop, rpc)
}

func (g gossipTracer) setMetricFromRPC(act action, subCtr prometheus.Counter, pubCtr, ctrlCtr *prometheus.CounterVec, rpc *pubsub.RPC) {
	subCtr.Add(float64(len(rpc.Subscriptions)))
	if rpc.Control != nil {
		ctrlCtr.WithLabelValues("graft").Add(float64(len(rpc.Control.Graft)))
		ctrlCtr.WithLabelValues("prune").Add(float64(len(rpc.Control.Prune)))
		ctrlCtr.WithLabelValues("ihave").Add(float64(len(rpc.Control.Ihave)))
		ctrlCtr.WithLabelValues("iwant").Add(float64(len(rpc.Control.Iwant)))
	}
	for _, msg := range rpc.Publish {
		// For incoming messages from pubsub, we do not record metrics for them as these values
		// could be junk.
		if act == recv {
			continue
		}
		pubCtr.WithLabelValues(*msg.Topic).Inc()
	}
}

func (g gossipTracer) flushTrace(evtType string, payload any) {
	evt := &traceEvent{
		Type:      evtType,
		PeerID:    g.host.ID(),
		Timestamp: time.Now(),
		Payload:   payload,
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	if err := g.producer.PutRecord(ctx, evt); err != nil {
		log.WithError(err).Warn("Failed to put trace event payload")
		return
	}
}

func (g gossipTracer) Trace(evt *pubsubpb.TraceEvent) {
	switch evt.GetType() {
	case pubsubpb.TraceEvent_RECV_RPC:
		payload := newRPCMeta(evt.GetRecvRPC().GetReceivedFrom(), evt.GetRecvRPC().GetMeta())
		g.flushTrace(pubsubpb.TraceEvent_RECV_RPC.String(), payload)
	case pubsubpb.TraceEvent_SEND_RPC:
		payload := newRPCMeta(evt.GetSendRPC().GetSendTo(), evt.GetSendRPC().GetMeta())
		g.flushTrace(pubsubpb.TraceEvent_SEND_RPC.String(), payload)
	case pubsubpb.TraceEvent_DROP_RPC:
		payload := newRPCMeta(evt.GetDropRPC().GetSendTo(), evt.GetDropRPC().GetMeta())
		g.flushTrace(pubsubpb.TraceEvent_DROP_RPC.String(), payload)
	}
}

type rpcMeta struct {
	PeerID        peer.ID
	Subscriptions []rpcMetaSub    `json:"Subs,omitempty"`
	Messages      []rpcMetaMsg    `json:"Msgs,omitempty"`
	Control       *rpcMetaControl `json:"Control,omitempty"`
}

type rpcMetaSub struct {
	Subscribe bool
	TopicID   string
}

type rpcMetaMsg struct {
	MsgID string `json:"MsgID,omitempty"`
	Topic string `json:"Topic,omitempty"`
}

type rpcMetaControl struct {
	IHave []rpcControlIHave `json:"IHave,omitempty"`
	IWant []rpcControlIWant `json:"IWant,omitempty"`
	Graft []rpcControlGraft `json:"Graft,omitempty"`
	Prune []rpcControlPrune `json:"Prune,omitempty"`
}

type rpcControlIHave struct {
	TopicID string
	MsgIDs  []string
}

type rpcControlIWant struct {
	MsgIDs []string
}

type rpcControlGraft struct {
	TopicID string
}

type rpcControlPrune struct {
	TopicID string
	PeerIDs []peer.ID
}

func newRPCMeta(pidBytes []byte, meta *pubsubpb.TraceEvent_RPCMeta) *rpcMeta {
	subs := make([]rpcMetaSub, len(meta.GetSubscription()))
	for i, subMeta := range meta.GetSubscription() {
		subs[i] = rpcMetaSub{
			Subscribe: subMeta.GetSubscribe(),
			TopicID:   subMeta.GetTopic(),
		}
	}

	msgs := make([]rpcMetaMsg, len(meta.GetMessages()))
	for i, msg := range meta.GetMessages() {
		msgs[i] = rpcMetaMsg{
			MsgID: hex.EncodeToString(msg.GetMessageID()),
			Topic: msg.GetTopic(),
		}
	}

	controlMsg := &rpcMetaControl{
		IHave: make([]rpcControlIHave, len(meta.GetControl().GetIhave())),
		IWant: make([]rpcControlIWant, len(meta.GetControl().GetIwant())),
		Graft: make([]rpcControlGraft, len(meta.GetControl().GetGraft())),
		Prune: make([]rpcControlPrune, len(meta.GetControl().GetPrune())),
	}

	for i, ihave := range meta.GetControl().GetIhave() {
		msgIDs := make([]string, len(ihave.GetMessageIDs()))
		for j, msgID := range ihave.GetMessageIDs() {
			msgIDs[j] = hex.EncodeToString(msgID)
		}

		controlMsg.IHave[i] = rpcControlIHave{
			TopicID: ihave.GetTopic(),
			MsgIDs:  msgIDs,
		}
	}

	for i, iwant := range meta.GetControl().GetIwant() {
		msgIDs := make([]string, len(iwant.GetMessageIDs()))
		for j, msgID := range iwant.GetMessageIDs() {
			msgIDs[j] = hex.EncodeToString(msgID)
		}

		controlMsg.IWant[i] = rpcControlIWant{
			MsgIDs: msgIDs,
		}
	}

	for i, graft := range meta.GetControl().GetGraft() {
		controlMsg.Graft[i] = rpcControlGraft{
			TopicID: graft.GetTopic(),
		}
	}

	for i, prune := range meta.GetControl().GetPrune() {

		peerIDs := make([]peer.ID, len(prune.GetPeers()))
		for j, peerIDBytes := range prune.GetPeers() {
			peerID, err := peer.IDFromBytes(peerIDBytes)
			if err != nil {
				log.WithError(err).Warn("failed parsing peer ID from prune msg")
				continue
			}

			peerIDs[j] = peerID
		}

		controlMsg.Prune[i] = rpcControlPrune{
			TopicID: prune.GetTopic(),
			PeerIDs: peerIDs,
		}
	}

	if len(controlMsg.IWant) == 0 && len(controlMsg.IHave) == 0 && len(controlMsg.Prune) == 0 && len(controlMsg.Graft) == 0 {
		controlMsg = nil
	}

	pid, err := peer.IDFromBytes(pidBytes)
	if err != nil {
		log.WithError(err).Warn("Failed parsing peer ID")
	}

	return &rpcMeta{
		PeerID:        pid,
		Subscriptions: subs,
		Messages:      msgs,
		Control:       controlMsg,
	}
}
