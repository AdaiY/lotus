package smm

import (
    "context"
    "fmt"
    "github.com/filecoin-project/lotus/api"
    "github.com/filecoin-project/lotus/build"
    "github.com/filecoin-project/lotus/chain/actors"
    laddress "github.com/filecoin-project/lotus/chain/address"
    "github.com/filecoin-project/lotus/chain/store"
    "github.com/filecoin-project/lotus/chain/types"
    "github.com/ipfs/go-cid"
    typegen "github.com/whyrusleeping/cbor-gen"
)

// Implements the storage mining module's Node interface in terms of the Lotus chain state.
type lotusAdapter struct {
    actor       laddress.Address    // storage miner address
    worker      laddress.Address    // worker address
    fullAPI     api.FullNode        // FullNode API
    listener    StateChangeHandler  // observer of state changes
}

func NewStorageMinerAdapter(fullapi api.FullNode, actor, worker Address, listener StateChangeHandler) (Node, error) {
    adapter := lotusAdapter{
        fullAPI:  fullapi,
        listener: listener,
    }
    var err error
    adapter.actor, err = laddress.NewFromString(string(actor))
    if err != nil {
        return nil, err
    }
    adapter.worker, err = laddress.NewFromString(string(worker))
    if err != nil {
        return nil, err
    }
    return adapter, nil
}

func stateChangesFromHeadChanges(changes []*store.HeadChange) []*StateChange {
    stateChanges := make([]*StateChange, len(changes))
    for idx, change := range changes {
        tsk := types.NewTipSetKey(change.Val.Cids()...)
        stateChanges[idx] = &StateChange{
            Epoch: Epoch(change.Val.Height()),
            StateKey: StateKey(tsk.Bytes()),
        }
    }
    return stateChanges
}

// this is expensive, it will be removed as FullNode transitions to use TipSetKey instead of TipSet
func statekey2tipset(ctx context.Context, stateKey StateKey, fullNode api.FullNode) (*types.TipSet, error) {
    tsk, err := types.TipSetKeyFromBytes([]byte(stateKey))
    if err != nil {
        return nil, err
    }
    cids := tsk.Cids()
    blockHeaders := make([]*types.BlockHeader, len(cids))
    for idx, blockCid := range cids {
        block, err := fullNode.ChainGetBlock(ctx, blockCid)
        if err != nil {
            return nil, err
        }
        blockHeaders[idx] = block
    }
    return types.NewTipSet(blockHeaders)
}

func (adapter lotusAdapter) eventHandler(ctx context.Context, headChanges <- chan[]*store.HeadChange) {
    for {
        select {
        case changes := <-headChanges:
            stateChanges := stateChangesFromHeadChanges(changes)
            for _, stateChange := range stateChanges {
                adapter.listener.OnChainStateChanged(stateChange)
            }

        case <-ctx.Done():
            return
        }
    }
}

func (adapter lotusAdapter) callMinerActorMethod(ctx context.Context, method uint64, params typegen.CBORMarshaler) (cid.Cid, error) {
    payload, aerr := actors.SerializeParams(params)
    if aerr != nil {
        return cid.Undef, aerr
    }
    msg := types.Message{
        To:       adapter.actor,
        From:     adapter.worker,
        Method:   method,
        Params:   payload,
        // TODO: Add ability to control these 'costs'
        Value:    types.NewInt(0),
        GasLimit: types.NewInt(1000000),
        GasPrice: types.NewInt(1),
    }
    signedMsg, err := adapter.fullAPI.MpoolPushMessage(ctx, &msg)
    if err != nil {
        return cid.Undef, err
    }
    return signedMsg.Cid(), nil
}

func (adapter lotusAdapter) Start(ctx context.Context) (*StateChange, error) {
    headChanges, err := adapter.fullAPI.ChainNotify(ctx)
    if err != nil {
        return nil, err
    }
    // read current state
    initialNotification := <- headChanges
    if len(initialNotification) != 1 {
        return nil, fmt.Errorf("unexpected initial head notification length: %d", len(initialNotification))
    }
    if initialNotification[0].Type != store.HCCurrent {
        return nil, fmt.Errorf("expected first head notification type to be 'current', was '%s'", initialNotification[0].Type)
    }
    go adapter.eventHandler(ctx, headChanges)
    stateChanges := stateChangesFromHeadChanges(initialNotification)
    return stateChanges[0], nil
}

func (adapter lotusAdapter) MostRecentState(ctx context.Context) (StateKey, Epoch, error) {
    ts, err := adapter.fullAPI.ChainHead(ctx)
    if err != nil {
        return "", Epoch(0), err
    }
    tsk := types.NewTipSetKey(ts.Cids()...)
    return StateKey(tsk.Bytes()), Epoch(ts.Height()), nil
}

func (adapter lotusAdapter) GetMinerState(ctx context.Context, stateKey StateKey) (*MinerChainState, error) {
    state := new(MinerChainState)
    ts, err := statekey2tipset(ctx, stateKey, adapter.fullAPI)
    if err != nil {
        return nil, err
    }
    sectors, err := adapter.fullAPI.StateMinerSectors(ctx, adapter.worker, ts)
    if err != nil {
        return nil, err
    }
    provingSectors, err := adapter.fullAPI.StateMinerProvingSet(ctx, adapter.worker, ts)
    if err != nil {
        return nil, err
    }
    state.Sectors = make(map[uint64]api.ChainSectorInfo)
    state.Address = Address(adapter.worker.String())
    state.PreCommittedSectors = make(map[uint64]api.ChainSectorInfo)
    state.StagedCommittedSectors = make(map[uint64]api.ChainSectorInfo)
    state.ProvingSet = make(map[uint64]struct{})
    for _, sectorInfo := range sectors {
        state.Sectors[sectorInfo.SectorID] = *sectorInfo
    }
    for _, sectorInfo := range provingSectors {
        state.ProvingSet[sectorInfo.SectorID] = struct{}{}
    }
    return state, nil
}

func (adapter lotusAdapter) GetRandomness(ctx context.Context, stateKey StateKey, offset int64) ([]byte, error) {
    ts, err := statekey2tipset(ctx, stateKey, adapter.fullAPI)
    if err != nil {
        return nil, err
    }
    tsk := types.NewTipSetKey(ts.Cids()...)
    return adapter.fullAPI.ChainGetRandomness(ctx, tsk, offset)
}

func (adapter lotusAdapter) GetProvingPeriod(ctx context.Context, stateKey StateKey) (*ProvingPeriod, error) {
    pp := new(ProvingPeriod)
    ts, err := statekey2tipset(ctx, stateKey, adapter.fullAPI)
    if err != nil {
        return nil, err
    }
    start, err := adapter.fullAPI.StateMinerElectionPeriodStart(ctx, adapter.worker, ts)
    if err != nil {
        return nil, err
    }
    pp.Start = Epoch(start)
    pp.End = Epoch(start + build.FallbackPoStDelay)
    return pp, nil
}

func (adapter lotusAdapter) SubmitSectorPreCommitment(ctx context.Context, id SectorID, sealEpoch Epoch, commR cid.Cid, dealIDs []uint64) (cid.Cid, error) {
    params := actors.SectorPreCommitInfo{
        SectorNumber: uint64(id),
        CommR: commR.Bytes(),
        SealEpoch: uint64(sealEpoch),
        DealIDs: dealIDs,
    }
    return adapter.callMinerActorMethod(ctx, actors.MAMethods.PreCommitSector, &params)
}

func (adapter lotusAdapter) SubmitSectorCommitment(ctx context.Context, id SectorID, proof Proof, dealIDs []uint64) (cid.Cid, error) {
    params := actors.SectorProveCommitInfo{
        Proof: proof,
        SectorID: uint64(id),
        DealIDs: dealIDs, // TODO: are the deal ids really needed here?
    }
    return adapter.callMinerActorMethod(ctx, actors.MAMethods.ProveCommitSector, &params)
}

func (adapter lotusAdapter) SubmitPoSt(ctx context.Context, proof Proof, candidates []EPostTicket) (cid.Cid, error) {
    params := actors.SubmitFallbackPoStParams{
        Proof:   proof,
        Candidates: make([]types.EPostTicket, len(candidates)),
    }
    for idx, candidate := range candidates {
        params.Candidates[idx].Partial = make([]byte, len(candidate.Partial))
        copy(params.Candidates[idx].Partial, candidate.Partial)
        params.Candidates[idx].SectorID = candidate.SectorID
        params.Candidates[idx].ChallengeIndex =  candidate.ChallengeIndex
    }
    return adapter.callMinerActorMethod(ctx, actors.MAMethods.SubmitFallbackPoSt, &params)
}

func (adapter lotusAdapter) SubmitDeclaredFaults(ctx context.Context, faults BitField) (cid.Cid, error) {
    params := actors.DeclareFaultsParams{
        Faults: types.NewBitField(),
    }
    for k, _ := range faults {
        params.Faults.Set(k)
    }
    return adapter.callMinerActorMethod(ctx, actors.MAMethods.DeclareFaults, &params)
}
