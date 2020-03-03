package sealing

import (
	"bytes"
	"context"
	"encoding/json"
	sectorbuilder "github.com/filecoin-project/go-sectorbuilder"
	"github.com/filecoin-project/go-sectorbuilder/fs"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"
	"io"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
)

func (m *Sealing) pledgeReader(size uint64, parts uint64) io.Reader {
	parts = 1 << bits.Len64(parts) // round down to nearest power of 2
	if size/parts < 127 {
		parts = size / 127
	}

	piece := sectorbuilder.UserBytesForSectorSize((size/127 + size) / parts)

	readers := make([]io.Reader, parts)
	for i := range readers {
		readers[i] = io.LimitReader(rand.New(rand.NewSource(42+int64(i))), int64(piece))
	}

	return io.MultiReader(readers...)
}

func (m *Sealing) pledgeSector(ctx context.Context, sectorID uint64, existingPieceSizes []uint64, sizes ...uint64) ([]Piece, error) {
	if len(sizes) == 0 {
		return nil, nil
	}

	log.Infof("Pledge %d, contains %+v", sectorID, existingPieceSizes)

	deals := make([]actors.StorageDealProposal, len(sizes))
	for i, size := range sizes {
		//commP, err := m.fastPledgeCommitment(size, uint64(1))
		commP, err := m.readCommPJson(sectorID, size)
		if err != nil {
			return nil, err
		}

		sdp := actors.StorageDealProposal{
			PieceRef:             commP[:],
			PieceSize:            size,
			Client:               m.worker,
			Provider:             m.maddr,
			ProposalExpiration:   math.MaxUint64,
			Duration:             math.MaxUint64 / 2, // /2 because overflows
			StoragePricePerEpoch: types.NewInt(0),
			StorageCollateral:    types.NewInt(0),
			ProposerSignature:    nil, // nil because self dealing
		}

		deals[i] = sdp
	}

	log.Infof("Publishing deals for %d", sectorID)

	params, aerr := actors.SerializeParams(&actors.PublishStorageDealsParams{
		Deals: deals,
	})
	if aerr != nil {
		return nil, xerrors.Errorf("serializing PublishStorageDeals params failed: ", aerr)
	}

	smsg, err := m.api.MpoolPushMessage(ctx, &types.Message{
		To:       actors.StorageMarketAddress,
		From:     m.worker,
		Value:    types.NewInt(0),
		GasPrice: types.NewInt(0),
		GasLimit: types.NewInt(1000000),
		Method:   actors.SMAMethods.PublishStorageDeals,
		Params:   params,
	})
	if err != nil {
		return nil, err
	}
	r, err := m.api.StateWaitMsg(ctx, smsg.Cid()) // TODO: more finality
	if err != nil {
		return nil, err
	}
	if r.Receipt.ExitCode != 0 {
		log.Error(xerrors.Errorf("publishing deal failed: exit %d", r.Receipt.ExitCode))
	}
	var resp actors.PublishStorageDealResponse
	if err := resp.UnmarshalCBOR(bytes.NewReader(r.Receipt.Return)); err != nil {
		return nil, err
	}
	if len(resp.DealIDs) != len(sizes) {
		return nil, xerrors.New("got unexpected number of DealIDs from PublishStorageDeals")
	}

	log.Infof("Deals for sector %d: %+v", sectorID, resp.DealIDs)

	out := make([]Piece, len(sizes))
	for i, size := range sizes {
		//ppi, err := m.sb.AddPiece(ctx, size, sectorID, m.pledgeReader(size, uint64(1)), existingPieceSizes)
		ppi, err := m.readPPIJson(ctx, sectorID, size, existingPieceSizes)
		if err != nil {
			return nil, xerrors.Errorf("add piece: %w", err)
		}

		existingPieceSizes = append(existingPieceSizes, size)

		out[i] = Piece{
			DealID: resp.DealIDs[i],
			Size:   ppi.Size,
			CommP:  ppi.CommP[:],
		}
	}

	return out, nil
}

func (m *Sealing) PledgeSector() error {
	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		size := sectorbuilder.UserBytesForSectorSize(m.sb.SectorSize())

		sid, err := m.sb.AcquireSectorId()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		pieces, err := m.pledgeSector(ctx, sid, []uint64{}, size)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		if err := m.newSector(context.TODO(), sid, pieces[0].DealID, pieces[0].ppi()); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}

type StagedCommP struct {
	CommP [sectorbuilder.CommLen]byte
	Path  string
}

func (m *Sealing) readCommPJson(sectorID uint64, size uint64) ([sectorbuilder.CommLen]byte, error) {
	lotusStoragePath, ex := os.LookupEnv("LOTUS_STORAGE_PATH")
	if !ex {
		lotusStoragePath = os.Getenv("HOME") + "/.lotusstorage"
	}
	CommPFile := lotusStoragePath + "/CommP.json"

	file, err := os.Open(CommPFile)

	if err != nil {
		if !os.IsNotExist(err) {
			return [sectorbuilder.CommLen]byte{}, xerrors.Errorf("comP json file os.OpenFile: %w", err)
		}

		commP, err := m.fastPledgeCommitment(size, uint64(1))
		if err != nil {
			return [sectorbuilder.CommLen]byte{}, err
		}
		stagedPath := fs.SectorPath(filepath.Join(lotusStoragePath, fs.SectorName(m.maddr, sectorID)))
		scf := StagedCommP{
			CommP: commP,
			Path:  string(stagedPath),
		}
		data, err := json.Marshal(scf)
		if err != nil {
			return [sectorbuilder.CommLen]byte{}, xerrors.Errorf("marshal stagedCommP json: %w", err)
		}

		if err := saveJson(data, CommPFile); err != nil {
			return [sectorbuilder.CommLen]byte{}, xerrors.Errorf("save commP json: %w", err)
		}

		return commP, err
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Warnf("comP json file closed failed: %w", err)
		}
	}()

	data := make([]byte, 2000)
	n, err := file.Read(data)
	if err != nil {
		return [sectorbuilder.CommLen]byte{}, xerrors.Errorf("read commP json: %w", err)
	}
	var sp StagedCommP
	if err := json.Unmarshal(data[:n], &sp); err != nil {
		return [sectorbuilder.CommLen]byte{}, xerrors.Errorf("unmarshal commP json: %w", err)
	}

	return sp.CommP, nil
}

func (m *Sealing) readPPIJson(ctx context.Context, sectorID uint64, size uint64, existingPieceSizes []uint64) (sectorbuilder.PublicPieceInfo, error) {
	lotusStoragePath, ex := os.LookupEnv("LOTUS_STORAGE_PATH")
	if !ex {
		lotusStoragePath = os.Getenv("HOME") + "/.lotusstorage"
	}
	PPIFile := lotusStoragePath + "/PPI.json"

	file, err := os.Open(PPIFile)

	if err != nil {
		if !os.IsNotExist(err) {
			return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("PPI json file os.OpenFile: %w", err)
		}

		ppi, err := m.sb.AddPiece(ctx, size, sectorID, m.pledgeReader(size, uint64(1)), existingPieceSizes)
		if err != nil {
			return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("add piece: %w", err)
		}

		data, err := json.Marshal(ppi)
		if err != nil {
			return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("marshal ppi json: %w", err)
		}
		if err := saveJson(data, PPIFile); err != nil {
			return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("save ppi json: %w", err)
		}

		stagedPathLast := fs.SectorPath(filepath.Join(lotusStoragePath, string(fs.DataStaging), fs.SectorName(m.maddr, sectorID)))
		stagedPath := fs.SectorPath(filepath.Join(lotusStoragePath, fs.SectorName(m.maddr, sectorID)))
		localStagedPath := fs.SectorPath(filepath.Join(os.Getenv("HOME")+"/.lotusstorage", string(fs.DataStaging), fs.SectorName(m.maddr, sectorID)))
		if err := os.Rename(string(stagedPathLast), string(stagedPath)); err != nil {
			return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("move staged sector: %w", err)
		}
		cmd := exec.Command("cp", "-rf", string(stagedPath), string(localStagedPath))
		log.Infof("copping staged sector: cp -rf %s %s", string(stagedPath), string(localStagedPath))
		if err := cmd.Run(); err != nil {
			return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("copy staged sector: %w", err)
		}
		if err := os.Symlink(string(localStagedPath), string(stagedPathLast)); err != nil {
			return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("create symlink: %w", err)
		}

		return ppi, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warnf("PPI json file closed failed: %w", err)
		}
	}()

	data := make([]byte, 2000)
	n, err := file.Read(data)
	if err != nil {
		return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("read ppi json: %w", err)
	}
	var ppi sectorbuilder.PublicPieceInfo
	if err := json.Unmarshal(data[:n], &ppi); err != nil {
		return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("unmarshal ppi json: %w", err)
	}

	return ppi, nil
}

func saveJson(data []byte, path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warnf("ppi json file closed failed: %w", err)
		}
	}()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}
