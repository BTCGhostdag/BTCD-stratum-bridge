package BTCDstratum

import (
	"context"
	"fmt"
	"time"

	"github.com/BTCGhostdag/BTCD-stratum-bridge/src/gostratum"
	"github.com/BTCGhostdag/BTCD/app/appmessage"
	"github.com/BTCGhostdag/BTCD/infrastructure/network/rpcclient"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type BTCDApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	BTCD      *rpcclient.RPCClient
	connected     bool
}

func NewBTCDAPI(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger) (*BTCDApi, error) {
	client, err := rpcclient.NewRPCClient(address)
	if err != nil {
		return nil, err
	}

	return &BTCDApi{
		address:       address,
		blockWaitTime: blockWaitTime,
		logger:        logger.With(zap.String("component", "BTCDapi:"+address)),
		BTCD:      client,
		connected:     true,
	}, nil
}

func (ks *BTCDApi) Start(ctx context.Context, blockCb func()) {
	ks.waitForSync(true)
	go ks.startBlockTemplateListener(ctx, blockCb)
	go ks.startStatsThread(ctx)
}

func (ks *BTCDApi) startStatsThread(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ks.logger.Warn("context cancelled, stopping stats thread")
			return
		case <-ticker.C:
			dagResponse, err := ks.BTCD.GetBlockDAGInfo()
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from BTCD, prom stats will be out of date", zap.Error(err))
				continue
			}
			response, err := ks.BTCD.EstimateNetworkHashesPerSecond(dagResponse.TipHashes[0], 1000)
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from BTCD, prom stats will be out of date", zap.Error(err))
				continue
			}
			RecordNetworkStats(response.NetworkHashesPerSecond, dagResponse.BlockCount, dagResponse.Difficulty)
		}
	}
}

func (ks *BTCDApi) reconnect() error {
	if ks.BTCD != nil {
		return ks.BTCD.Reconnect()
	}

	client, err := rpcclient.NewRPCClient(ks.address)
	if err != nil {
		return err
	}
	ks.BTCD = client
	return nil
}

func (s *BTCDApi) waitForSync(verbose bool) error {
	if verbose {
		s.logger.Info("checking BTCD sync state")
	}
	for {
		clientInfo, err := s.BTCD.GetInfo()
		if err != nil {
			return errors.Wrapf(err, "error fetching server info from BTCD @ %s", s.address)
		}
		if clientInfo.IsSynced {
			break
		}
		s.logger.Warn("BTCD is not synced, waiting for sync before starting bridge")
		time.Sleep(5 * time.Second)
	}
	if verbose {
		s.logger.Info("BTCD synced, starting server")
	}
	return nil
}

func (s *BTCDApi) startBlockTemplateListener(ctx context.Context, blockReadyCb func()) {
	blockReadyChan := make(chan bool)
	err := s.BTCD.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
		blockReadyChan <- true
	})
	if err != nil {
		s.logger.Error("fatal: failed to register for block notifications from BTCD")
	}

	ticker := time.NewTicker(s.blockWaitTime)
	for {
		if err := s.waitForSync(false); err != nil {
			s.logger.Error("error checking BTCD sync state, attempting reconnect: ", err)
			if err := s.reconnect(); err != nil {
				s.logger.Error("error reconnecting to BTCD, waiting before retry: ", err)
				time.Sleep(5 * time.Second)
			}
		}
		select {
		case <-ctx.Done():
			s.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-blockReadyChan:
			blockReadyCb()
			ticker.Reset(s.blockWaitTime)
		case <-ticker.C: // timeout, manually check for new blocks
			blockReadyCb()
		}
	}
}

func (ks *BTCDApi) GetBlockTemplate(
	client *gostratum.StratumContext) (*appmessage.GetBlockTemplateResponseMessage, error) {
	template, err := ks.BTCD.GetBlockTemplate(client.WalletAddr,
		fmt.Sprintf(`'%s' via BTCGhostdag/BTCD-stratum-bridge_%s`, client.RemoteApp, version))
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from BTCD")
	}
	return template, nil
}
