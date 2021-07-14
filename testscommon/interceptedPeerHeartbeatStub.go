package testscommon

// InterceptedPeerHeartbeatStub -
type InterceptedPeerHeartbeatStub struct {
	InterceptedDataStub
	PublicKeyCalled          func() []byte
	SetComputedShardIDCalled func(shardId uint32)
}

// PublicKey -
func (i *InterceptedPeerHeartbeatStub) PublicKey() []byte {
	if i.PublicKeyCalled != nil {
		return i.PublicKeyCalled()
	}

	return make([]byte, 0)
}

// SetComputedShardID -
func (i *InterceptedPeerHeartbeatStub) SetComputedShardID(shardId uint32) {
	if i.SetComputedShardIDCalled != nil {
		i.SetComputedShardIDCalled(shardId)
	}
}
