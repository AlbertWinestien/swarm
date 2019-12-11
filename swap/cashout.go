package swap

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p/enode"
	contract "github.com/ethersphere/swarm/contracts/swap"
	"github.com/ethersphere/swarm/state"
)

// CashoutLoop holds all relevent fields needed for the cashout go routine
type CashoutLoop struct {
	lock    sync.Mutex           // lock for the loop (used to prevent double-closing of channels)
	queue   chan *CashoutRequest // channel for future cashout requests
	context context.Context      // context used for the loop
	cancel  context.CancelFunc   // cancel function of context
	done    chan bool            // channel for signalling that the loop has terminated
	cashed  chan *CashoutRequest // channel for successful cashouts

	store      state.Store       // state store to save and load the active cashout from
	backend    contract.Backend  // ethereum backend to use
	privateKey *ecdsa.PrivateKey // private key to use
}

type CashoutRequest struct {
	Cheque      Cheque
	Peer        enode.ID
	Destination common.Address
}

type ActiveCashout struct {
	Request         CashoutRequest
	TransactionHash common.Hash
}

func newCashoutLoop(store state.Store, backend contract.Backend, privateKey *ecdsa.PrivateKey) *CashoutLoop {
	context, cancel := context.WithCancel(context.Background())
	return &CashoutLoop{
		store:      store,
		backend:    backend,
		privateKey: privateKey,
		done:       make(chan bool),
		queue:      make(chan *CashoutRequest, 50),
		cashed:     make(chan *CashoutRequest, 50),
		context:    context,
		cancel:     cancel,
	}
}

func (c *CashoutLoop) queueRequest(cashoutRequest *CashoutRequest) {
	select {
	case c.queue <- cashoutRequest:
	default:
		swapLog.Warn("attempting to write cashout request to closed queue")
	}
}

func (c *CashoutLoop) start() {
	go c.loop()
}

func (c *CashoutLoop) stop() {
	c.lock.Lock()
	select {
	case <-c.context.Done():
		return
	default:
	}
	c.cancel()
	c.lock.Unlock()

	close(c.queue)
	<-c.done
	close(c.done)
}

func (c *CashoutLoop) loop() {
	activeCashout, err := c.loadActiveCashout()
	if err != nil {
		swapLog.Error(err.Error())
		return
	}

	if activeCashout != nil {
		c.processActiveCashout(activeCashout)
		err = c.saveActiveCashout(nil)
		if err != nil {
			log.Error(err.Error())
			return // TODO: stop processing for now
		}
	}

	for request := range c.queue {
		ctx, cancel := context.WithTimeout(c.context, DefaultTransactionTimeout)
		defer cancel()

		cheque := request.Cheque

		otherSwap, err := contract.InstanceAt(cheque.Contract, c.backend)
		if err != nil {
			swapLog.Error("error getting contract", "err", err)
			continue
		}

		paidOut, err := otherSwap.PaidOut(&bind.CallOpts{Context: ctx}, cheque.Beneficiary)
		if err != nil {
			swapLog.Error("error getting paidout", "err", err)
			continue
		}

		// skip if already cashed
		if paidOut.Cmp(big.NewInt(int64(cheque.CumulativePayout))) > 0 {
			continue
		}

		gasPrice, err := c.backend.SuggestGasPrice(ctx)
		if err != nil {
			swapLog.Error("error getting gasprice", "err", err)
			continue
		}
		transactionCosts := gasPrice.Uint64() * 50000 // cashing a cheque is approximately 50000 gas
		// do a payout transaction if we get 2 times the gas costs
		if (cheque.CumulativePayout - paidOut.Uint64()) > 2*transactionCosts {
			opts := bind.NewKeyedTransactor(c.privateKey)
			opts.Context = ctx

			tx, err := otherSwap.CashChequeBeneficiaryStart(opts, request.Destination, big.NewInt(int64(cheque.CumulativePayout)), cheque.Signature)
			if err != nil {
				swapLog.Error("failed to cash cheque", "err", err)
				c.queueRequest(request)
				continue
			}

			activeCashout := &ActiveCashout{
				Request:         *request,
				TransactionHash: tx.Hash(),
			}

			err = c.saveActiveCashout(activeCashout)
			if err != nil {
				log.Error(err.Error())
				return // TODO: stop processing for now
			}

			c.processActiveCashout(activeCashout)

			err = c.saveActiveCashout(nil)
			if err != nil {
				log.Error(err.Error())
			}
		}
	}

	c.done <- true
}

// processActiveCashout waits for activeCashout to complete and reenter into the queuee if necessary
func (c *CashoutLoop) processActiveCashout(activeCashout *ActiveCashout) {
	ctx, cancel := context.WithTimeout(c.context, DefaultTransactionTimeout)
	defer cancel()

	receipt, err := awaitTransactionByHash(ctx, c.backend, activeCashout.TransactionHash)
	if err != nil {
		swapLog.Error("failed to cash cheque", "err", err)
		c.queueRequest(&activeCashout.Request)
		return
	}

	otherSwap, err := contract.InstanceAt(activeCashout.Request.Cheque.Contract, c.backend)
	if err != nil {
		swapLog.Error("error getting contract", "err", err)
		c.queueRequest(&activeCashout.Request)
		return
	}

	result := otherSwap.CashChequeBeneficiaryResult(receipt)

	metrics.GetOrRegisterCounter("swap.cheques.cashed.honey", nil).Inc(result.TotalPayout.Int64())

	if result.Bounced {
		metrics.GetOrRegisterCounter("swap.cheques.cashed.bounced", nil).Inc(1)
		swapLog.Warn("cheque bounced", "tx", receipt.TxHash)
	}

	swapLog.Info("cheque cashed", "honey", activeCashout.Request.Cheque.Honey)

	select {
	case c.cashed <- &activeCashout.Request:
	default:
		log.Error("cashed channel full")
	}
}

// awaitTransactionByHash waits for a transaction to by mined by hash
func awaitTransactionByHash(ctx context.Context, backend contract.Backend, txHash common.Hash) (*types.Receipt, error) {
	tx, pending, err := backend.TransactionByHash(ctx, txHash)
	if err != nil {
		return nil, err
	}

	var receipt *types.Receipt
	if pending {
		receipt, err = contract.WaitFunc(ctx, backend, tx)
		if err != nil {
			return nil, err
		}
	} else {
		receipt, err = backend.TransactionReceipt(ctx, txHash)
		if err != nil {
			return nil, err
		}
	}

	return receipt, nil
}

// loadActiveCashout loads the activeCashout from the store
func (c *CashoutLoop) loadActiveCashout() (activeCashout *ActiveCashout, err error) {
	err = c.store.Get("cashout_loop_active", &activeCashout)
	if err == state.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return activeCashout, nil
}

// saveActiveCashout saves activeCashout to the store
func (c *CashoutLoop) saveActiveCashout(activeCashout *ActiveCashout) error {
	return c.store.Put("cashout_loop_active", activeCashout)
}
