package debug

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	// EngineAPIGetBlobsResponse is sent after a blob request to the EL engine API has been received
	EngineAPIGetBlobsResponse = iota + 1
)

// BlockGossipReceivedData is the data sent after the CL asks the El for a given set of Blobs.
type EngineAPIGetBlobsResponseData struct {
	// SignedBlock is the block that was received.
	Timestamp           time.Time
	ReqDuration         time.Duration
	ValDuration         time.Duration
	ReconstructDuration time.Duration
	Request             []common.Hash
	Response            []bool
	SuccessArray        []bool
	Error               string
}
