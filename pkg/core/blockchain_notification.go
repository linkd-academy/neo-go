package core

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/config/limits"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/mempool"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
)

// notificationDispatcher manages subscription to events and broadcasts new events.
func (bc *Blockchain) notificationDispatcher() {
	var (
		// These are just sets of subscribers, though modelled as maps
		// for ease of management (not a lot of subscriptions is really
		// expected, but maps are convenient for adding/deleting elements).
		blockFeed        = make(map[chan *block.Block]bool)
		headerFeed       = make(map[chan *block.Header]bool)
		txFeed           = make(map[chan *transaction.Transaction]bool)
		notificationFeed = make(map[chan *state.ContainedNotificationEvent]bool)
		executionFeed    = make(map[chan *state.AppExecResult]bool)
	)
	for {
		select {
		case <-bc.stopCh:
			return
		case sub := <-bc.subCh:
			switch ch := sub.(type) {
			case chan *block.Header:
				headerFeed[ch] = true
			case chan *block.Block:
				blockFeed[ch] = true
			case chan *transaction.Transaction:
				txFeed[ch] = true
			case chan *state.ContainedNotificationEvent:
				notificationFeed[ch] = true
			case chan *state.AppExecResult:
				executionFeed[ch] = true
			default:
				panic(fmt.Sprintf("bad subscription: %T", sub))
			}
		case unsub := <-bc.unsubCh:
			switch ch := unsub.(type) {
			case chan *block.Header:
				delete(headerFeed, ch)
			case chan *block.Block:
				delete(blockFeed, ch)
			case chan *transaction.Transaction:
				delete(txFeed, ch)
			case chan *state.ContainedNotificationEvent:
				delete(notificationFeed, ch)
			case chan *state.AppExecResult:
				delete(executionFeed, ch)
			default:
				panic(fmt.Sprintf("bad unsubscription: %T", unsub))
			}
		case event := <-bc.events:
			// We don't want to waste time looping through transactions when there are no
			// subscribers.
			if len(txFeed) != 0 || len(notificationFeed) != 0 || len(executionFeed) != 0 {
				aer := event.appExecResults[0]
				if !aer.Container.Equals(event.block.Hash()) {
					panic("inconsistent application execution results")
				}
				for ch := range executionFeed {
					ch <- aer
				}
				for i := range aer.Events {
					for ch := range notificationFeed {
						ch <- &state.ContainedNotificationEvent{
							Container:         aer.Container,
							NotificationEvent: aer.Events[i],
						}
					}
				}

				aerIdx := 1
				for _, tx := range event.block.Transactions {
					aer := event.appExecResults[aerIdx]
					if !aer.Container.Equals(tx.Hash()) {
						panic("inconsistent application execution results")
					}
					aerIdx++
					for ch := range executionFeed {
						ch <- aer
					}
					if aer.VMState == vmstate.Halt {
						for i := range aer.Events {
							for ch := range notificationFeed {
								ch <- &state.ContainedNotificationEvent{
									Container:         aer.Container,
									NotificationEvent: aer.Events[i],
								}
							}
						}
					}
					for ch := range txFeed {
						ch <- tx
					}
				}

				aer = event.appExecResults[aerIdx]
				if !aer.Container.Equals(event.block.Hash()) {
					panic("inconsistent application execution results")
				}
				for ch := range executionFeed {
					ch <- aer
				}
				for i := range aer.Events {
					for ch := range notificationFeed {
						ch <- &state.ContainedNotificationEvent{
							Container:         aer.Container,
							NotificationEvent: aer.Events[i],
						}
					}
				}
			}
			for ch := range headerFeed {
				ch <- &event.block.Header
			}
			for ch := range blockFeed {
				ch <- event.block
			}
		}
	}
}

func (bc *Blockchain) handleNotification(note *state.NotificationEvent, d *dao.Simple,
	transCache map[util.Uint160]transferData, b *block.Block, h util.Uint256) {
	if note.Name != "Transfer" {
		return
	}
	arr, ok := note.Item.Value().([]stackitem.Item)
	if !ok || !(len(arr) == 3 || len(arr) == 4) {
		return
	}
	from, err := parseUint160(arr[0])
	if err != nil {
		return
	}
	to, err := parseUint160(arr[1])
	if err != nil {
		return
	}
	amount, err := arr[2].TryInteger()
	if err != nil {
		return
	}
	var id []byte
	if len(arr) == 4 {
		id, err = arr[3].TryBytes()
		if err != nil || len(id) > limits.MaxStorageKeyLen {
			return
		}
	}

	d.PutTransactionForHash160(from, note.Item)
	d.PutTransactionForHash160(to, h)


	bc.processTokenTransfer(d, transCache, h, b, note.ScriptHash, from, to, amount, id)
}

func parseUint160(itm stackitem.Item) (util.Uint160, error) {
	_, ok := itm.(stackitem.Null) // Minting or burning.
	if ok {
		return util.Uint160{}, nil
	}
	bytes, err := itm.TryBytes()
	if err != nil {
		return util.Uint160{}, err
	}
	return util.Uint160DecodeBytesBE(bytes)
}

// RegisterPostBlock appends provided function to the list of functions which should be run after new block
// is stored.
func (bc *Blockchain) RegisterPostBlock(f func(func(*transaction.Transaction, *mempool.Pool, bool) bool, *mempool.Pool, *block.Block)) {
	bc.postBlock = append(bc.postBlock, f)
}
// SubscribeForBlocks adds given channel to new block event broadcasting, so when
// there is a new block added to the chain you'll receive it via this channel.
// Make sure it's read from regularly as not reading these events might affect
// other Blockchain functions. Make sure you're not changing the received blocks,
// as it may affect the functionality of Blockchain and other subscribers.
func (bc *Blockchain) SubscribeForBlocks(ch chan *block.Block) {
	bc.subCh <- ch
}

// SubscribeForHeadersOfAddedBlocks adds given channel to new header event broadcasting, so
// when there is a new block added to the chain you'll receive its header via this
// channel. Make sure it's read from regularly as not reading these events might
// affect other Blockchain functions. Make sure you're not changing the received
// headers, as it may affect the functionality of Blockchain and other
// subscribers.
func (bc *Blockchain) SubscribeForHeadersOfAddedBlocks(ch chan *block.Header) {
	bc.subCh <- ch
}

// SubscribeForTransactions adds given channel to new transaction event
// broadcasting, so when there is a new transaction added to the chain (in a
// block) you'll receive it via this channel. Make sure it's read from regularly
// as not reading these events might affect other Blockchain functions. Make sure
// you're not changing the received transactions, as it may affect the
// functionality of Blockchain and other subscribers.
func (bc *Blockchain) SubscribeForTransactions(ch chan *transaction.Transaction) {
	bc.subCh <- ch
}

// SubscribeForNotifications adds given channel to new notifications event
// broadcasting, so when an in-block transaction execution generates a
// notification you'll receive it via this channel. Only notifications from
// successful transactions are broadcasted, if you're interested in failed
// transactions use SubscribeForExecutions instead. Make sure this channel is
// read from regularly as not reading these events might affect other Blockchain
// functions. Make sure you're not changing the received notification events, as
// it may affect the functionality of Blockchain and other subscribers.
func (bc *Blockchain) SubscribeForNotifications(ch chan *state.ContainedNotificationEvent) {
	bc.subCh <- ch
}

// SubscribeForExecutions adds given channel to new transaction execution event
// broadcasting, so when an in-block transaction execution happens you'll receive
// the result of it via this channel. Make sure it's read from regularly as not
// reading these events might affect other Blockchain functions. Make sure you're
// not changing the received execution results, as it may affect the
// functionality of Blockchain and other subscribers.
func (bc *Blockchain) SubscribeForExecutions(ch chan *state.AppExecResult) {
	bc.subCh <- ch
}

// UnsubscribeFromBlocks unsubscribes given channel from new block notifications,
// you can close it afterwards. Passing non-subscribed channel is a no-op, but
// the method can read from this channel (discarding any read data).
func (bc *Blockchain) UnsubscribeFromBlocks(ch chan *block.Block) {
unsubloop:
	for {
		select {
		case <-ch:
		case bc.unsubCh <- ch:
			break unsubloop
		}
	}
}

// UnsubscribeFromHeadersOfAddedBlocks unsubscribes given channel from new
// block's header notifications, you can close it afterwards. Passing
// non-subscribed channel is a no-op, but the method can read from this
// channel (discarding any read data).
func (bc *Blockchain) UnsubscribeFromHeadersOfAddedBlocks(ch chan *block.Header) {
unsubloop:
	for {
		select {
		case <-ch:
		case bc.unsubCh <- ch:
			break unsubloop
		}
	}
}

// UnsubscribeFromTransactions unsubscribes given channel from new transaction
// notifications, you can close it afterwards. Passing non-subscribed channel is
// a no-op, but the method can read from this channel (discarding any read data).
func (bc *Blockchain) UnsubscribeFromTransactions(ch chan *transaction.Transaction) {
unsubloop:
	for {
		select {
		case <-ch:
		case bc.unsubCh <- ch:
			break unsubloop
		}
	}
}

// UnsubscribeFromNotifications unsubscribes given channel from new
// execution-generated notifications, you can close it afterwards. Passing
// non-subscribed channel is a no-op, but the method can read from this channel
// (discarding any read data).
func (bc *Blockchain) UnsubscribeFromNotifications(ch chan *state.ContainedNotificationEvent) {
unsubloop:
	for {
		select {
		case <-ch:
		case bc.unsubCh <- ch:
			break unsubloop
		}
	}
}

// UnsubscribeFromExecutions unsubscribes given channel from new execution
// notifications, you can close it afterwards. Passing non-subscribed channel is
// a no-op, but the method can read from this channel (discarding any read data).
func (bc *Blockchain) UnsubscribeFromExecutions(ch chan *state.AppExecResult) {
unsubloop:
	for {
		select {
		case <-ch:
		case bc.unsubCh <- ch:
			break unsubloop
		}
	}
}

