package testscommon

import (
	"math/big"

	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/headerVersionData"
)

// HeaderHandlerStub -
type HeaderHandlerStub struct {
	EpochField                             uint32
	TimestampField                         uint64
	GetMiniBlockHeadersWithDstCalled       func(destId uint32) map[string]uint32
	GetOrderedCrossMiniblocksWithDstCalled func(destId uint32) []*data.MiniBlockInfo
	GetPubKeysBitmapCalled                 func() []byte
	GetSignatureCalled                     func() []byte
	GetRootHashCalled                      func() []byte
	GetRandSeedCalled                      func() []byte
	GetPrevRandSeedCalled                  func() []byte
	GetPrevHashCalled                      func() []byte
	CloneCalled                            func() data.HeaderHandler
	GetChainIDCalled                       func() []byte
	CheckChainIDCalled                     func(reference []byte) error
	GetReservedCalled                      func() []byte
	IsStartOfEpochBlockCalled              func() bool
}

// GetAccumulatedFees -
func (hhs *HeaderHandlerStub) GetAccumulatedFees() *big.Int {
	return big.NewInt(0)
}

// GetDeveloperFees -
func (hhs *HeaderHandlerStub) GetDeveloperFees() *big.Int {
	return big.NewInt(0)
}

// SetAccumulatedFees -
func (hhs *HeaderHandlerStub) SetAccumulatedFees(_ *big.Int) error {
	return nil
}

// SetDeveloperFees -
func (hhs *HeaderHandlerStub) SetDeveloperFees(_ *big.Int) error {
	return nil
}

// GetReceiptsHash -
func (hhs *HeaderHandlerStub) GetReceiptsHash() []byte {
	return []byte("receipt")
}

// SetShardID -
func (hhs *HeaderHandlerStub) SetShardID(_ uint32) error {
	return nil
}

// IsStartOfEpochBlock -
func (hhs *HeaderHandlerStub) IsStartOfEpochBlock() bool {
	if hhs.IsStartOfEpochBlockCalled != nil {
		return hhs.IsStartOfEpochBlockCalled()
	}

	return false
}

// ShallowClone -
func (hhs *HeaderHandlerStub) ShallowClone() data.HeaderHandler {
	return hhs.CloneCalled()
}

// GetShardID -
func (hhs *HeaderHandlerStub) GetShardID() uint32 {
	return 1
}

// GetNonce -
func (hhs *HeaderHandlerStub) GetNonce() uint64 {
	return 1
}

// GetEpoch -
func (hhs *HeaderHandlerStub) GetEpoch() uint32 {
	return hhs.EpochField
}

// GetRound -
func (hhs *HeaderHandlerStub) GetRound() uint64 {
	return 1
}

// GetTimeStamp -
func (hhs *HeaderHandlerStub) GetTimeStamp() uint64 {
	return hhs.TimestampField
}

// GetRootHash -
func (hhs *HeaderHandlerStub) GetRootHash() []byte {
	return hhs.GetRootHashCalled()
}

// GetPrevHash -
func (hhs *HeaderHandlerStub) GetPrevHash() []byte {
	return hhs.GetPrevHashCalled()
}

// GetPrevRandSeed -
func (hhs *HeaderHandlerStub) GetPrevRandSeed() []byte {
	return hhs.GetPrevRandSeedCalled()
}

// GetRandSeed -
func (hhs *HeaderHandlerStub) GetRandSeed() []byte {
	return hhs.GetRandSeedCalled()
}

// GetPubKeysBitmap -
func (hhs *HeaderHandlerStub) GetPubKeysBitmap() []byte {
	return hhs.GetPubKeysBitmapCalled()
}

// GetSignature -
func (hhs *HeaderHandlerStub) GetSignature() []byte {
	return hhs.GetSignatureCalled()
}

// GetLeaderSignature -
func (hhs *HeaderHandlerStub) GetLeaderSignature() []byte {
	return hhs.GetSignatureCalled()
}

// GetChainID -
func (hhs *HeaderHandlerStub) GetChainID() []byte {
	return hhs.GetChainIDCalled()
}

// GetTxCount -
func (hhs *HeaderHandlerStub) GetTxCount() uint32 {
	return 0
}

// GetReserved -
func (hhs *HeaderHandlerStub) GetReserved() []byte {
	if hhs.GetReservedCalled != nil {
		return hhs.GetReservedCalled()
	}

	return nil
}

// SetNonce -
func (hhs *HeaderHandlerStub) SetNonce(_ uint64) error {
	panic("implement me")
}

// SetEpoch -
func (hhs *HeaderHandlerStub) SetEpoch(_ uint32) error {
	panic("implement me")
}

// SetRound -
func (hhs *HeaderHandlerStub) SetRound(_ uint64) error {
	panic("implement me")
}

// SetTimeStamp -
func (hhs *HeaderHandlerStub) SetTimeStamp(_ uint64) error {
	panic("implement me")
}

// SetRootHash -
func (hhs *HeaderHandlerStub) SetRootHash(_ []byte) error {
	panic("implement me")
}

// SetPrevHash -
func (hhs *HeaderHandlerStub) SetPrevHash(_ []byte) error {
	panic("implement me")
}

// SetPrevRandSeed -
func (hhs *HeaderHandlerStub) SetPrevRandSeed(_ []byte) error {
	panic("implement me")
}

// SetRandSeed -
func (hhs *HeaderHandlerStub) SetRandSeed(_ []byte) error {
	panic("implement me")
}

// SetPubKeysBitmap -
func (hhs *HeaderHandlerStub) SetPubKeysBitmap(_ []byte) error {
	panic("implement me")
}

// SetSignature -
func (hhs *HeaderHandlerStub) SetSignature(_ []byte) error {
	panic("implement me")
}

// SetLeaderSignature -
func (hhs *HeaderHandlerStub) SetLeaderSignature(_ []byte) error {
	panic("implement me")
}

// SetChainID -
func (hhs *HeaderHandlerStub) SetChainID(_ []byte) error {
	panic("implement me")
}

// SetTxCount -
func (hhs *HeaderHandlerStub) SetTxCount(_ uint32) error {
	panic("implement me")
}

// GetMiniBlockHeadersWithDst -
func (hhs *HeaderHandlerStub) GetMiniBlockHeadersWithDst(destId uint32) map[string]uint32 {
	return hhs.GetMiniBlockHeadersWithDstCalled(destId)
}

// GetOrderedCrossMiniblocksWithDst -
func (hhs *HeaderHandlerStub) GetOrderedCrossMiniblocksWithDst(destId uint32) []*data.MiniBlockInfo {
	return hhs.GetOrderedCrossMiniblocksWithDstCalled(destId)
}

// GetMiniBlockHeadersHashes -
func (hhs *HeaderHandlerStub) GetMiniBlockHeadersHashes() [][]byte {
	panic("implement me")
}

// GetMiniBlockHeaderHandlers -
func (hhs *HeaderHandlerStub) GetMiniBlockHeaderHandlers() []data.MiniBlockHeaderHandler {
	panic("implement me")
}

// GetMetaBlockHashes -
func (hhs *HeaderHandlerStub) GetMetaBlockHashes() [][]byte {
	panic("implement me")
}

// GetBlockBodyTypeInt32 -
func (hhs *HeaderHandlerStub) GetBlockBodyTypeInt32() int32 {
	panic("implement me")
}

// GetValidatorStatsRootHash -
func (hhs *HeaderHandlerStub) GetValidatorStatsRootHash() []byte {
	return []byte("vs root hash")
}

// SetValidatorStatsRootHash -
func (hhs *HeaderHandlerStub) SetValidatorStatsRootHash(_ []byte) error {
	panic("implement me")
}

// SetMiniBlockHeaderHandlers -
func (hhs *HeaderHandlerStub) SetMiniBlockHeaderHandlers(_ []data.MiniBlockHeaderHandler) error {
	panic("implement me")
}

// IsInterfaceNil returns true if there is no value under the interface
func (hhs *HeaderHandlerStub) IsInterfaceNil() bool {
	return hhs == nil
}

// GetEpochStartMetaHash -
func (hhs *HeaderHandlerStub) GetEpochStartMetaHash() []byte {
	panic("implement me")
}

// GetSoftwareVersion -
func (hhs *HeaderHandlerStub) GetSoftwareVersion() []byte {
	return []byte("softwareVersion")
}

// SetSoftwareVersion -
func (hhs *HeaderHandlerStub) SetSoftwareVersion(_ []byte) error {
	return nil
}

// SetReceiptsHash -
func (hhs *HeaderHandlerStub) SetReceiptsHash(_ []byte) error {
	return nil
}

// SetMetaBlockHashes -
func (hhs *HeaderHandlerStub) SetMetaBlockHashes(_ [][]byte) error {
	return nil
}

// SetEpochStartMetaHash -
func (h *HeaderHandlerStub) SetEpochStartMetaHash(_ []byte) error {
	return nil
}

// GetShardInfoHandlers -
func (hhs *HeaderHandlerStub) GetShardInfoHandlers() []data.ShardDataHandler {
	panic("implement me")
}

// GetEpochStartHandler -
func (hhs *HeaderHandlerStub) GetEpochStartHandler() data.EpochStartHandler {
	panic("implement me")
}

// GetDevFeesInEpoch -
func (hhs *HeaderHandlerStub) GetDevFeesInEpoch() *big.Int {
	panic("implement me")
}

// SetDevFeesInEpoch -
func (hhs *HeaderHandlerStub) SetDevFeesInEpoch(_ *big.Int) error {
	panic("implement me")
}

// SetShardInfoHandlers -
func (hhs *HeaderHandlerStub) SetShardInfoHandlers(_ []data.ShardDataHandler) error {
	panic("implement me")
}

// SetAccumulatedFeesInEpoch -
func (hhs *HeaderHandlerStub) SetAccumulatedFeesInEpoch(_ *big.Int) error {
	panic("implement me")
}

// SetScheduledRootHash -
func (hhs *HeaderHandlerStub) SetScheduledRootHash(rootHash []byte) error {
	panic("implement me")
}

// ValidateHeaderVersion -
func (hhs *HeaderHandlerStub) ValidateHeaderVersion() error {
	return nil
}

// SetAdditionalData sets the additional version-related data
func (hhs *HeaderHandlerStub) SetAdditionalData(_ headerVersionData.HeaderAdditionalData) error {
	return nil
}

// HasScheduledSupport -
func (hhs *HeaderHandlerStub) HasScheduledSupport() bool {
	return false
}

// GetAdditionalData -
func (hhs *HeaderHandlerStub) GetAdditionalData() headerVersionData.HeaderAdditionalData {
	return nil
}
