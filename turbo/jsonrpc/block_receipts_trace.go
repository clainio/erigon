package jsonrpc

import (
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"

	"github.com/ledgerwatch/erigon/common/math"

	"github.com/ledgerwatch/erigon/rpc"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

const enable_testing = false

func calculateBorRewards(block_validator *common.Address, accumulated_fee uint256.Int) ([]consensus.Reward, error) {
	if block_validator != nil {
		var consensus_rewards []consensus.Reward

		block_reward := consensus.Reward{
			Beneficiary: *block_validator,
			Kind:        consensus.RewardAuthor,
			Amount:      accumulated_fee,
		}

		consensus_rewards = append(consensus_rewards, block_reward)
		return consensus_rewards, nil
	}
	return nil, fmt.Errorf("no valid block validator or block trxs fees  == 0")
}

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
		var legacyTx *types.LegacyTx

		if txJson.To != nil {
			toAddr = *txJson.To
			legacyTx = types.NewTransaction(uint64(txJson.Nonce), toAddr, value, uint64(txJson.Gas), gasPrice, txJson.Input)
		} else {
			legacyTx = types.NewContractCreation(uint64(txJson.Nonce), value, uint64(txJson.Gas), gasPrice, txJson.Input)
		}

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

	case types.BlobTxType:
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

		blobTx := types.BlobTx{DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce: uint64(txJson.Nonce),
				Gas:   uint64(txJson.Gas),
				To:    txJson.To,
				Value: value,
				Data:  txJson.Input,
			},
			ChainID:    chainId,
			Tip:        tip,
			FeeCap:     feeCap,
			AccessList: *txJson.Accesses,
		},
			BlobVersionedHashes: txJson.BlobVersionedHashes,
		}

		blobTx.MaxFeePerBlobGas.SetFromBig(txJson.MaxFeePerBlobGas.ToInt())

		blobTx.V.SetFromBig(txJson.V.ToInt())
		blobTx.S.SetFromBig(txJson.S.ToInt())
		blobTx.R.SetFromBig(txJson.R.ToInt())

		return &blobTx, nil

	default:
		return nil, nil
	}
}

type APIEthTraceImpl struct {
	APIImpl
	traceImpl *TraceAPIImpl
	borImpl   *BorImpl
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

func NewEthTraceAPI(base *BaseAPI, traceImpl *TraceAPIImpl, borImpl *BorImpl, db kv.RoDB, eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, gascap uint64, returnDataLimit int, allowUnprotectedTxs bool, maxGetProofRewindBlockCount int, logger log.Logger) *APIEthTraceImpl {
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
		borImpl:   borImpl,
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

	var block_validator *common.Address
	var block_validator_err error

	if chainConfig.Bor != nil {
		if api.borImpl != nil {
			block_validator, block_validator_err = api.borImpl.GetAuthor(&numberOrHash)
			if block_validator_err != nil {
				return nil, fmt.Errorf("validator for Polygon error %w", block_validator_err)
			}
			block_trxs_enriched["miner"] = block_validator

		} else {
			return nil, fmt.Errorf("requested validator for Polygon chain, but BorImpl == nil")
		}
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

	var borTx types.Transaction
	if chainConfig.Bor != nil {
		borTx = rawdb.ReadBorTransactionForBlock(tx, blockNum)
		if borTx != nil {
			borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), blockNum, receipts)
			if err != nil {
				return nil, err
			}
			if borReceipt != nil {
				bor_receipts := marshalReceipt(borReceipt, borTx, chainConfig, block.HeaderNoCopy(), borReceipt.TxHash, false)
				if bor_clean_err := CleanLogs(bor_receipts); bor_clean_err != nil {
					log.Error("could not clean logs", "error", bor_clean_err)
				}

				result = append(result, bor_receipts)
			}
		}
	}

	trxs_len := len(block_trxs_enriched["transactions"].([]interface{}))

	var accumulated_fee uint256.Int

	for i := 0; i < trxs_len; i++ {
		trx := block_trxs_enriched["transactions"].([]interface{})[i].(*ethapi.RPCTransaction)
		if trx.Hash != result[i]["transactionHash"] {
			return nil, fmt.Errorf("transaction hash mismatch for transaction %d, trx number %d", *numberOrHash.BlockNumber, i)
		}
		trx.Receipts = result[i]

		if chainConfig.Bor != nil {
			trx_gas_used := result[i]["gasUsed"].(hexutil.Uint64).Uint64()
			local_trx_fee := new(big.Int).Mul((*big.Int)(trx.GasPrice), new(big.Int).SetUint64(trx_gas_used))
			local_trx_fee_265, overflow := uint256.FromBig(local_trx_fee)
			if overflow {
				return nil, fmt.Errorf("overflow in calculating trx fee")
			}

			accumulated_fee.Add(&accumulated_fee, local_trx_fee_265)
		}
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

	var rewards []consensus.Reward
	var rewards_err error
	if chainConfig.Bor == nil {
		rewards, rewards_err = api.engine().CalculateRewards(chainConfig, block.Header(), block.Uncles(), syscall)
	} else {
		rewards, rewards_err = calculateBorRewards(block_validator, accumulated_fee)
	}
	if rewards_err != nil {
		return nil, rewards_err
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

	trxs_in_block := block.Transactions()

	if borTx != nil && len(trxs_in_block) == 0 {
		if trxs_len != 1 {
			return nil, fmt.Errorf("missmatch between bor trx and total trx number in block")
		}

		trxs_in_block = make(types.Transactions, 0)
		trxs_in_block = append(trxs_in_block, borTx)
	}

	for i := 0; i < trxs_len; i++ {
		trx := block_trxs_enriched["transactions"].([]interface{})[i].(*ethapi.RPCTransaction)
		trx.Trace = result_trace[i]

		if trxs_len == 1 && borTx != nil {
			break
		}

		_, pub_key, signer_err := signer.Sender(trxs_in_block[i])
		if signer_err != nil && borTx == nil {
			return nil, fmt.Errorf("cannot get pub key for block %d, trx index %d", *numberOrHash.BlockNumber, i)
		}

		ecdsa_pubkey, ecdsa_pubkey_err := crypto.UnmarshalPubkeyStd(pub_key)
		if ecdsa_pubkey_err != nil {
			return nil, fmt.Errorf("cannot get ECDSA pub key for block %d, trx index %d, error: %s", *numberOrHash.BlockNumber, i, ecdsa_pubkey_err.Error())
		}

		compressed_pubkey := crypto.CompressPubkey(ecdsa_pubkey)

		if enable_testing {
			tx_hash := trxs_in_block[i].SigningHash(nil)

			sig := make([]byte, 64)
			copy(sig[32-len(trx.R.ToInt().Bytes()):32], trx.R.ToInt().Bytes())
			copy(sig[64-len(trx.S.ToInt().Bytes()):64], trx.S.ToInt().Bytes())

			verified := crypto.VerifySignature(compressed_pubkey, tx_hash[:], sig)
			if !verified {
				fmt.Println(tx_hash)
			}

			dec_pub_key, _ := crypto.DecompressPubkey(compressed_pubkey)
			recovered_address := crypto.PubkeyToAddress(*dec_pub_key)
			if recovered_address != trx.From {
				fmt.Println(recovered_address)
			}
		}

		trx.PubKey = common.PubKeyCompressedType(compressed_pubkey)
	}

	return block_trxs_enriched, nil
}
