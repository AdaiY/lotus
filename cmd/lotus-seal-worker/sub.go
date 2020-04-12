package main

import (
	"context"
	"encoding/json"
	"github.com/filecoin-project/go-sectorbuilder/fs"
	"github.com/filecoin-project/lotus/storage/sealing"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/filecoin-project/go-sectorbuilder"
	"golang.org/x/xerrors"

	lapi "github.com/filecoin-project/lotus/api"
)

type worker struct {
	api           lapi.StorageMiner
	minerEndpoint string
	repo          string
	auth          http.Header

	limiter *limits
	sb      *sectorbuilder.SectorBuilder
}

func acceptJobs(ctx context.Context, api lapi.StorageMiner, sb *sectorbuilder.SectorBuilder, limiter *limits, endpoint string, auth http.Header, repo string, noprecommit, nocommit bool) error {
	w := &worker{
		api:           api,
		minerEndpoint: endpoint,
		auth:          auth,
		repo:          repo,

		limiter: limiter,
		sb:      sb,
	}

	tasks, err := api.WorkerQueue(ctx, sectorbuilder.WorkerCfg{
		NoPreCommit: noprecommit,
		NoCommit:    nocommit,
	})
	if err != nil {
		return err
	}

loop:
	for {
		log.Infof("Waiting for new task")

		select {
		case task := <-tasks:
			log.Infof("New task: %d, sector %d, action: %d", task.TaskID, task.SectorID, task.Type)

			res := w.processTask(ctx, task)

			log.Infof("Task %d done, err: %+v", task.TaskID, res.GoErr)

			if err := api.WorkerDone(ctx, task.TaskID, res); err != nil {
				log.Error(err)
			}
		case <-ctx.Done():
			break loop
		}
	}

	log.Warn("acceptJobs exit")
	return nil
}

func (w *worker) processTask(ctx context.Context, task sectorbuilder.WorkerTask) sectorbuilder.SealRes {
	switch task.Type {
	case sectorbuilder.WorkerPreCommit:
	case sectorbuilder.WorkerCommit:
	default:
		return errRes(xerrors.Errorf("unknown task type %d", task.Type))
	}

	if task.Type == sectorbuilder.WorkerPreCommit {
		if err := w.fetchStagedSector(); err != nil {
			return errRes(xerrors.Errorf("fetching staged sector: %w", err))
		}
	}

	log.Infof("Data fetched, starting computation")

	var res sectorbuilder.SealRes

	switch task.Type {
	case sectorbuilder.WorkerPreCommit:
		w.limiter.workLimit <- struct{}{}
		rspco, err := w.sb.SealPreCommit(ctx, task.SectorID, task.SealTicket, task.Pieces)
		<-w.limiter.workLimit

		if err != nil {
			return errRes(xerrors.Errorf("precomitting: %w", err))
		}
		res.Rspco = rspco.ToJson()

		if err := w.copySealedSector(task.SectorID); err != nil {
			return errRes(xerrors.Errorf("pushing precommited data: %w", err))
		}

	case sectorbuilder.WorkerCommit:
		w.limiter.workLimit <- struct{}{}
		proof, err := w.sb.SealCommit(ctx, task.SectorID, task.SealTicket, task.SealSeed, task.Pieces, task.Rspco)
		<-w.limiter.workLimit

		if err != nil {
			return errRes(xerrors.Errorf("comitting: %w", err))
		}

		res.Proof = proof

		if err := w.moveCache(task.SectorID); err != nil {
			return errRes(xerrors.Errorf("pushing precommited data: %w", err))
		}

		if err := w.removeSealedSector(task.SectorID); err != nil {
			return errRes(xerrors.Errorf("cleaning up sealed sector: %w", err))
		}
	}

	return res
}

func errRes(err error) sectorbuilder.SealRes {
	return sectorbuilder.SealRes{Err: err.Error(), GoErr: err}
}

func (w *worker) fetchStagedSector() error {
	workerPath, ex := os.LookupEnv("WORKER_PATH")
	if !ex {
		workerPath = os.Getenv("HOME") + "/.lotusworker"
	}
	CommPFile := workerPath + "/CommP.json"

	file, err := os.Open(CommPFile)
	if err != nil {
		return xerrors.Errorf("comP json file os.OpenFile: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warnf("comP json file closed failed: %w", err)
		}
	}()
	data := make([]byte, 2000)
	n, err := file.Read(data)
	if err != nil {
		return xerrors.Errorf("read commP json: %w", err)
	}
	var sp sealing.StagedCommP
	if err := json.Unmarshal(data[:n], &sp); err != nil {
		return xerrors.Errorf("unmarshal commP json: %w", err)
	}

	localStoragePath := os.Getenv("HOME") + "/.lotusstorage" + sp.Path[strings.LastIndex(sp.Path, "/"):]

	_, err = os.Stat(localStoragePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return xerrors.Errorf("sector file stat: %w", err)
		}
		cmd := exec.Command("cp", "-rf", sp.Path, localStoragePath)
		log.Infof("copping staged sector: cp -rf %s %s", sp.Path, localStoragePath)
		if err := cmd.Run(); err != nil {
			return xerrors.Errorf("copy staged sector: %w", err)
		}
	}

	return nil
}

func (w *worker) copySealedSector(sectorID uint64) error {
	workerPath, ex := os.LookupEnv("WORKER_PATH")
	if !ex {
		workerPath = os.Getenv("HOME") + "/.lotusworker"
	}

	sealedPath := fs.SectorPath(filepath.Join(workerPath, string(fs.DataSealed), fs.SectorName(w.sb.Miner, sectorID)))
	localSealedPath := fs.SectorPath(filepath.Join(os.Getenv("HOME")+"/.lotusstorage/", string(fs.DataLocalSealed), fs.SectorName(w.sb.Miner, sectorID)))

	cmd := exec.Command("cp", "-rf", string(localSealedPath), string(sealedPath))
	log.Infof("copping sealed sector: cp -rf %s %s", string(localSealedPath), string(sealedPath))
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("copy sealed sector: %w", err)
	}

	return nil
}

func (w *worker) removeSealedSector(sectorID uint64) error {
	localSealedPath := fs.SectorPath(filepath.Join(os.Getenv("HOME")+"/.lotusstorage/", string(fs.DataLocalSealed), fs.SectorName(w.sb.Miner, sectorID)))
	if err := os.Remove(string(localSealedPath)); err != nil {
		return xerrors.Errorf("remove sealed sector: %w", err)
	}
	return nil
}

func (w *worker) moveCache(sectorID uint64) error {
	workerPath, ex := os.LookupEnv("WORKER_PATH")
	if !ex {
		workerPath = os.Getenv("HOME") + "/.lotusworker"
	}
	cachePath := fs.SectorPath(filepath.Join(workerPath, string(fs.DataCache), fs.SectorName(w.sb.Miner, sectorID)))
	localCachePath := fs.SectorPath(filepath.Join(os.Getenv("HOME")+"/.lotusstorage/", string(fs.DataLocalCache), fs.SectorName(w.sb.Miner, sectorID)))

	cmd := exec.Command("mv", "-f", string(localCachePath), string(cachePath))
	log.Infof("moving sector cache: mv -f %s %s", string(localCachePath), string(cachePath))
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("move sector cache: %w", err)
	}

	return nil
}
