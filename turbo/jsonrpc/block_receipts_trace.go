package jsonrpc

import (
	//"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
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
	//"github.com/ledgerwatch/erigon/crypto"
)

func getETHTransaction(txJson *ethapi.RPCTransaction) (types.Transaction, error) {
	gasPrice, value := uint256.NewInt(0), uint256.NewInt(0)
	var overflow bool
	var chainId *uint256.Int

	if txJson.Value != nil {
		value, overflow = uint256.FromBig((*big.Int)(txJson.Value))
		if overflow {
			return nil, fmt.Errorf("value field caused an overflow (uint256)")
		}
	}

	if txJson.GasPrice != nil {
		gasPrice, overflow = uint256.FromBig((*big.Int)(txJson.GasPrice))
		if overflow {
			return nil, fmt.Errorf("gasPrice field caused an overflow (uint256)")
		}
	}

	if txJson.ChainID != nil {
		chainId, overflow = uint256.FromBig((*big.Int)(txJson.ChainID))
		if overflow {
			return nil, fmt.Errorf("chainId field caused an overflow (uint256)")
		}
	}

	switch txJson.Type {
	case types.LegacyTxType, types.AccessListTxType:
		var toAddr = common.Address{}
		if txJson.To != nil {
			toAddr = *txJson.To
		}
		legacyTx := types.NewTransaction(uint64(txJson.Nonce), toAddr, value, uint64(txJson.Gas), gasPrice, txJson.Input)
		legacyTx.V.SetFromBig(txJson.V.ToInt())
		legacyTx.S.SetFromBig(txJson.S.ToInt())
		legacyTx.R.SetFromBig(txJson.R.ToInt())

		if txJson.Type == types.AccessListTxType {
			accessListTx := types.AccessListTx{
				LegacyTx:   *legacyTx,
				ChainID:    chainId,
				AccessList: *txJson.Accesses,
			}

			return &accessListTx, nil
		} else {
			return legacyTx, nil
		}

	case types.DynamicFeeTxType:
		var tip *uint256.Int
		var feeCap *uint256.Int
		if txJson.Tip != nil {
			tip, overflow = uint256.FromBig((*big.Int)(txJson.Tip))
			if overflow {
				return nil, fmt.Errorf("maxPriorityFeePerGas field caused an overflow (uint256)")
			}
		}

		if txJson.FeeCap != nil {
			feeCap, overflow = uint256.FromBig((*big.Int)(txJson.FeeCap))
			if overflow {
				return nil, fmt.Errorf("maxFeePerGas field caused an overflow (uint256)")
			}
		}

		dynamicFeeTx := types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce: uint64(txJson.Nonce),
				To:    txJson.To,
				Value: value,
				Gas:   uint64(txJson.Gas),
				Data:  txJson.Input,
			},
			ChainID:    chainId,
			Tip:        tip,
			FeeCap:     feeCap,
			AccessList: *txJson.Accesses,
		}

		dynamicFeeTx.V.SetFromBig(txJson.V.ToInt())
		dynamicFeeTx.S.SetFromBig(txJson.S.ToInt())
		dynamicFeeTx.R.SetFromBig(txJson.R.ToInt())

		return &dynamicFeeTx, nil

	default:
		return nil, nil
	}
}

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

		eth_trx, eth_trx_err := getETHTransaction(trx)
		if eth_trx_err != nil {
			return nil, fmt.Errorf("cannot get ETH trx from RPC trx for block %d, trx index %d", *numberOrHash.BlockNumber, i)
		}

		_, pub_key, signer_err := signer.Sender(eth_trx)
		if signer_err != nil {
			return nil, fmt.Errorf("cannot get pub key for block %d, trx index %d", *numberOrHash.BlockNumber, i)
		}

		trx.PubKey = common.PubKeyType(pub_key)
	}

	return block_trxs_enriched, nil
}
