package jsonrpc

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"

	"github.com/ledgerwatch/erigon/common/math"

	"github.com/ledgerwatch/erigon/rpc"

	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

type APIEthTraceImpl struct {
	APIImpl
	traceImpl *TraceAPIImpl
}

func CleanLogs(full_logs_result map[string]interface{}) error {
	var clean_logs types.CleanLogs

	logs_interface, ok := full_logs_result["logs"]
	if ok {
		switch logs := logs_interface.(type) {
		case types.Logs:
			logs_typed := logs

			for _, log := range logs_typed {
				clean_log := &types.CleanLog{
					Address: log.Address,
					Topics:  log.Topics,
					Data:    log.Data,
					Index:   log.Index,
					Removed: log.Removed,
				}
				clean_logs = append(clean_logs, clean_log)
			}

			delete(full_logs_result, "logs")
			full_logs_result["logs"] = clean_logs

			return nil
		case types.Log:

			return nil
		}
	}
	return nil
}

func NewEthTraceAPI(base *BaseAPI, traceImpl *TraceAPIImpl, db kv.RoDB, eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, gascap uint64, returnDataLimit int, allowUnprotectedTxs bool, maxGetProofRewindBlockCount int, logger log.Logger) *APIEthTraceImpl {
	var gas_cap uint64
	if gascap == 0 {
		gas_cap = uint64(math.MaxUint64 / 2)
	}

	return &APIEthTraceImpl{
		APIImpl: APIImpl{
			BaseAPI:                     base,
			db:                          db,
			ethBackend:                  eth,
			txPool:                      txPool,
			mining:                      mining,
			gasCache:                    NewGasPriceCache(),
			GasCap:                      gas_cap,
			AllowUnprotectedTxs:         allowUnprotectedTxs,
			ReturnDataLimit:             returnDataLimit,
			MaxGetProofRewindBlockCount: maxGetProofRewindBlockCount,
			logger:                      logger,
		},
		traceImpl: traceImpl,
	}
}

func (api *APIEthTraceImpl) GetBlockReceiptsTrace(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) (map[string]interface{}, error) {
	block_trxs_enriched, block_trxs_err := api.APIImpl.GetBlockByNumber(ctx, *numberOrHash.BlockNumber, true)
	delete(block_trxs_enriched, "hash")
	delete(block_trxs_enriched, "number")

	if block_trxs_err != nil {
		return nil, block_trxs_err
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, blockHash, _, err := rpchelper.GetBlockNumber(numberOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block  %d", blockNum)
	}
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx, chainConfig, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}

	result := make([]map[string]interface{}, 0, len(receipts))

	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]

		full_result := marshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true)
		if clean_err := CleanLogs(full_result); clean_err != nil {
			log.Error("could not clean logs", "error", clean_err)
		}

		result = append(result, full_result)
	}

	if chainConfig.Bor != nil {
		borTx := rawdb.ReadBorTransactionForBlock(tx, blockNum)
		if borTx != nil {
			borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), blockNum, receipts)
			if err != nil {
				return nil, err
			}
			if borReceipt != nil {
				result = append(result, marshalReceipt(borReceipt, borTx, chainConfig, block.HeaderNoCopy(), borReceipt.TxHash, false))
			}
		}
	}

	trxs_len := len(block_trxs_enriched["transactions"].([]interface{}))

	for i := 0; i < trxs_len; i++ {
		trx := block_trxs_enriched["transactions"].([]interface{})[i].(*ethapi.RPCTransaction)
		if trx.Hash != result[i]["transactionHash"] {
			return nil, fmt.Errorf("transaction hash mismatch for transaction %d, trx number %d", *numberOrHash.BlockNumber, i)
		}
		trx.Receipts = result[i]
	}

	var gasBailOut *bool
	if gasBailOut == nil {
		gasBailOut = new(bool)
	}

	traceTypes := []string{"trace"}
	var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
	traceTypeTrace = true

	signer := types.MakeSigner(chainConfig, blockNum, block.Time())
	traces, syscall, err := api.traceImpl.callManyTransactions(ctx, tx, block, traceTypes, -1 /* all tx indices */, *gasBailOut, signer, chainConfig)
	if err != nil {
		if len(result) > 0 {
			return block_trxs_enriched, nil
		}
		return nil, err
	}

	result_trace := make([]*TraceCallResult, len(traces))
	for i, trace := range traces {
		tr := &TraceCallResult{}
		tr.Output = trace.Output
		if traceTypeTrace {
			tr.Trace = trace.Trace
		} else {
			tr.Trace = []*ParityTrace{}
		}
		if traceTypeStateDiff {
			tr.StateDiff = trace.StateDiff
		}
		if traceTypeVmTrace {
			tr.VmTrace = trace.VmTrace
		}
		result_trace[i] = tr
	}

	rewards, err := api.engine().CalculateRewards(chainConfig, block.Header(), block.Uncles(), syscall)
	if err != nil {
		return nil, err
	}

	parity_traces := make([]ParityTrace, 0)

	for _, r := range rewards {
		var tr ParityTrace
		rewardAction := &RewardTraceAction{}
		rewardAction.Author = r.Beneficiary
		rewardAction.RewardType = rewardKindToString(r.Kind)
		rewardAction.Value.ToInt().Set(r.Amount.ToBig())
		tr.Action = rewardAction
		tr.BlockHash = &common.Hash{}
		copy(tr.BlockHash[:], block.Hash().Bytes())
		tr.BlockNumber = new(uint64)
		*tr.BlockNumber = block.NumberU64()
		tr.Type = "reward" // nolint: goconst
		tr.TraceAddress = []int{}
		parity_traces = append(parity_traces, tr)
	}

	if len(rewards) > 0 {
		block_trxs_enriched["rewards"] = parity_traces
	}

	for i := 0; i < trxs_len; i++ {
		trx := block_trxs_enriched["transactions"].([]interface{})[i].(*ethapi.RPCTransaction)
		trx.Trace = result_trace[i]
	}

	return block_trxs_enriched, nil
}
