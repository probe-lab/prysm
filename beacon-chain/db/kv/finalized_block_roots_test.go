package kv

import (
	"context"
	"testing"

	fieldparams "github.com/prysmaticlabs/prysm/v4/config/fieldparams"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	consensusblocks "github.com/prysmaticlabs/prysm/v4/consensus-types/blocks"
	"github.com/prysmaticlabs/prysm/v4/consensus-types/interfaces"
	"github.com/prysmaticlabs/prysm/v4/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v4/encoding/bytesutil"
	ethpb "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v4/testing/assert"
	"github.com/prysmaticlabs/prysm/v4/testing/require"
	"github.com/prysmaticlabs/prysm/v4/testing/util"
	bolt "go.etcd.io/bbolt"
)

var genesisBlockRoot = bytesutil.ToBytes32([]byte{'G', 'E', 'N', 'E', 'S', 'I', 'S'})

func TestStore_IsFinalizedBlock(t *testing.T) {
	slotsPerEpoch := uint64(params.BeaconConfig().SlotsPerEpoch)
	db := setupDB(t)
	ctx := context.Background()

	require.NoError(t, db.SaveGenesisBlockRoot(ctx, genesisBlockRoot))
	blks := makeBlocks(t, 0, slotsPerEpoch*2, genesisBlockRoot)
	require.NoError(t, db.SaveBlocks(ctx, blks))

	root, err := blks[slotsPerEpoch].Block().HashTreeRoot()
	require.NoError(t, err)
	cp := &ethpb.Checkpoint{
		Epoch: 1,
		Root:  root[:],
	}
	require.NoError(t, db.SaveFinalizedCheckpoint(ctx, cp))

	for i := uint64(0); i <= slotsPerEpoch; i++ {
		root, err = blks[i].Block().HashTreeRoot()
		require.NoError(t, err)
		assert.Equal(t, true, db.IsFinalizedBlock(ctx, root), "Block at index %d was not considered finalized", i)
	}
	for i := slotsPerEpoch + 1; i < uint64(len(blks)); i++ {
		root, err = blks[i].Block().HashTreeRoot()
		require.NoError(t, err)
		assert.Equal(t, false, db.IsFinalizedBlock(ctx, root), "Block at index %d was considered finalized, but should not have", i)
	}
}

func TestStore_IsFinalizedGenesisBlock(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	blk := util.NewBeaconBlock()
	blk.Block.Slot = 0
	root, err := blk.Block.HashTreeRoot()
	require.NoError(t, err)
	wsb, err := consensusblocks.NewSignedBeaconBlock(blk)
	require.NoError(t, err)
	require.NoError(t, db.SaveBlock(ctx, wsb))
	require.NoError(t, db.SaveGenesisBlockRoot(ctx, root))
	assert.Equal(t, true, db.IsFinalizedBlock(ctx, root))
}

func TestStore_IsFinalizedChildBlock(t *testing.T) {
	slotsPerEpoch := uint64(params.BeaconConfig().SlotsPerEpoch)
	ctx := context.Background()
	db := setupDB(t)
	require.NoError(t, db.SaveGenesisBlockRoot(ctx, genesisBlockRoot))

	blks := makeBlocks(t, 0, slotsPerEpoch*2, genesisBlockRoot)
	require.NoError(t, db.SaveBlocks(ctx, blks))
	root, err := blks[slotsPerEpoch].Block().HashTreeRoot()
	require.NoError(t, err)
	cp := &ethpb.Checkpoint{
		Epoch: 1,
		Root:  root[:],
	}
	require.NoError(t, db.SaveFinalizedCheckpoint(ctx, cp))

	for i := uint64(0); i < slotsPerEpoch; i++ {
		root, err = blks[i].Block().HashTreeRoot()
		require.NoError(t, err)
		assert.Equal(t, true, db.IsFinalizedBlock(ctx, root), "Block at index %d was not considered finalized", i)
		blk, err := db.FinalizedChildBlock(ctx, root)
		assert.NoError(t, err)
		assert.Equal(t, false, blk == nil, "Child block at index %d was not considered finalized", i)
	}
}

func TestStore_ChildRootOfPrevFinalizedCheckpointIsUpdated(t *testing.T) {
	slotsPerEpoch := uint64(params.BeaconConfig().SlotsPerEpoch)
	ctx := context.Background()
	db := setupDB(t)
	require.NoError(t, db.SaveGenesisBlockRoot(ctx, genesisBlockRoot))

	blks := makeBlocks(t, 0, slotsPerEpoch*3, genesisBlockRoot)
	require.NoError(t, db.SaveBlocks(ctx, blks))
	root, err := blks[slotsPerEpoch].Block().HashTreeRoot()
	require.NoError(t, err)
	cp := &ethpb.Checkpoint{
		Epoch: 1,
		Root:  root[:],
	}
	require.NoError(t, db.SaveFinalizedCheckpoint(ctx, cp))
	root2, err := blks[slotsPerEpoch*2].Block().HashTreeRoot()
	require.NoError(t, err)
	cp = &ethpb.Checkpoint{
		Epoch: 2,
		Root:  root2[:],
	}
	require.NoError(t, db.SaveFinalizedCheckpoint(ctx, cp))

	require.NoError(t, db.db.View(func(tx *bolt.Tx) error {
		container := &ethpb.FinalizedBlockRootContainer{}
		f := tx.Bucket(finalizedBlockRootsIndexBucket).Get(root[:])
		require.NoError(t, decode(ctx, f, container))
		r, err := blks[slotsPerEpoch+1].Block().HashTreeRoot()
		require.NoError(t, err)
		assert.DeepEqual(t, r[:], container.ChildRoot)
		return nil
	}))
}

func TestStore_OrphanedBlockIsNotFinalized(t *testing.T) {
	slotsPerEpoch := uint64(params.BeaconConfig().SlotsPerEpoch)
	db := setupDB(t)
	ctx := context.Background()

	require.NoError(t, db.SaveGenesisBlockRoot(ctx, genesisBlockRoot))
	blk0 := util.NewBeaconBlock()
	blk0.Block.ParentRoot = genesisBlockRoot[:]
	blk0Root, err := blk0.Block.HashTreeRoot()
	require.NoError(t, err)
	blk1 := util.NewBeaconBlock()
	blk1.Block.Slot = 1
	blk1.Block.ParentRoot = blk0Root[:]
	blk2 := util.NewBeaconBlock()
	blk2.Block.Slot = 2
	// orphan block at index 1
	blk2.Block.ParentRoot = blk0Root[:]
	blk2Root, err := blk2.Block.HashTreeRoot()
	require.NoError(t, err)
	sBlk0, err := consensusblocks.NewSignedBeaconBlock(blk0)
	require.NoError(t, err)
	sBlk1, err := consensusblocks.NewSignedBeaconBlock(blk1)
	require.NoError(t, err)
	sBlk2, err := consensusblocks.NewSignedBeaconBlock(blk2)
	require.NoError(t, err)
	blks := append([]interfaces.ReadOnlySignedBeaconBlock{sBlk0, sBlk1, sBlk2}, makeBlocks(t, 3, slotsPerEpoch*2-3, blk2Root)...)
	require.NoError(t, db.SaveBlocks(ctx, blks))

	root, err := blks[slotsPerEpoch].Block().HashTreeRoot()
	require.NoError(t, err)
	cp := &ethpb.Checkpoint{
		Epoch: 1,
		Root:  root[:],
	}
	require.NoError(t, db.SaveFinalizedCheckpoint(ctx, cp))

	for i := uint64(0); i <= slotsPerEpoch; i++ {
		root, err = blks[i].Block().HashTreeRoot()
		require.NoError(t, err)
		if i == 1 {
			assert.Equal(t, false, db.IsFinalizedBlock(ctx, root), "Block at index 1 was considered finalized, but should not have")
		} else {
			assert.Equal(t, true, db.IsFinalizedBlock(ctx, root), "Block at index %d was not considered finalized", i)
		}
	}
}

func makeBlocks(t *testing.T, i, n uint64, previousRoot [32]byte) []interfaces.ReadOnlySignedBeaconBlock {
	blocks := make([]*ethpb.SignedBeaconBlock, n)
	ifaceBlocks := make([]interfaces.ReadOnlySignedBeaconBlock, n)
	for j := i; j < n+i; j++ {
		parentRoot := make([]byte, fieldparams.RootLength)
		copy(parentRoot, previousRoot[:])
		blocks[j-i] = util.NewBeaconBlock()
		blocks[j-i].Block.Slot = primitives.Slot(j + 1)
		blocks[j-i].Block.ParentRoot = parentRoot
		var err error
		previousRoot, err = blocks[j-i].Block.HashTreeRoot()
		require.NoError(t, err)
		ifaceBlocks[j-i], err = consensusblocks.NewSignedBeaconBlock(blocks[j-i])
		require.NoError(t, err)
	}
	return ifaceBlocks
}
