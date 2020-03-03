package main

import (
	"context"
	"encoding/json"
	"github.com/filecoin-project/lotus/storage/sealing"
	"net/http"
	"os"
	"os/exec"
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

	//if err := w.fetchSector(task.SectorID, task.Type); err != nil {
	//	return errRes(xerrors.Errorf("fetching sector: %w", err))
	//}
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

		//if err := w.push("sealed", task.SectorID); err != nil {
		//	return errRes(xerrors.Errorf("pushing precommited data: %w", err))
		//}
		//
		//if err := w.push("cache", task.SectorID); err != nil {
		//	return errRes(xerrors.Errorf("pushing precommited data: %w", err))
		//}
		//
		//if err := w.remove("staging", task.SectorID); err != nil {
		//	return errRes(xerrors.Errorf("cleaning up staged sector: %w", err))
		//}
	case sectorbuilder.WorkerCommit:
		w.limiter.workLimit <- struct{}{}
		proof, err := w.sb.SealCommit(ctx, task.SectorID, task.SealTicket, task.SealSeed, task.Pieces, task.Rspco)
		<-w.limiter.workLimit

		if err != nil {
			return errRes(xerrors.Errorf("comitting: %w", err))
		}

		res.Proof = proof

		//if err := w.push("cache", task.SectorID); err != nil {
		//	return errRes(xerrors.Errorf("pushing precommited data: %w", err))
		//}
		//
		//if err := w.remove("sealed", task.SectorID); err != nil {
		//	return errRes(xerrors.Errorf("cleaning up sealed sector: %w", err))
		//}
	}

	return res
}

func errRes(err error) sectorbuilder.SealRes {
	return sectorbuilder.SealRes{Err: err.Error(), GoErr: err}
}

func (w *worker) fetchStagedSector() error {
	lotusStoragePath := os.Getenv("HOME") + "/.lotusworker"
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

	_, err = os.Stat(sp.Path)
	if err != nil {
		if !os.IsNotExist(err) {
			return xerrors.Errorf("sector file stat: %w", err)
		}
		cmd := exec.Command("cp", "-rf", sp.Path, lotusStoragePath+sp.Path[strings.LastIndex(sp.Path, "/"):])
		log.Infof("copping staged sector: cp -rf %s %s", sp.Path, lotusStoragePath+sp.Path[strings.LastIndex(sp.Path, "/"):])
		if err := cmd.Run(); err != nil {
			return xerrors.Errorf("copy staged sector: %w", err)
		}
	}

	return nil
}
