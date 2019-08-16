package ring

import (
	"context"
	"sync/atomic"
)

type batchTracker struct {
	rpcsPending int32
	rpcsFailed  int32
	done        chan struct{}
	err         chan error
}

type ingester struct {
	// 某个ingester以及和它相关的各个item
	desc         IngesterDesc
	itemTrackers []*itemTracker
	indexes      []int
}

// itemTracker记录最小成功数，最大失败数以及实际的成功数和失败数
type itemTracker struct {
	minSuccess  int
	maxFailures int
	succeeded   int32
	failed      int32
}

// DoBatch request against a set of keys in the ring, handling replication and
// failures. For example if we want to write N items where they may all
// hit different ingesters, and we want them all replicated R ways with
// quorum writes, we track the relationship between batch RPCs and the items
// within them.
// DoBatch请求ring中的一系列keys，处理replication以及failures，比如，如果我们想要写N个items，它们可能会hit不同的ingesters
// 我们希望它们都重复R份以满足quorum writes，我们追踪batch RPC以及items之前的关系
//
// Callback is passed the ingester to target, and the indexes of the keys
// to send to that ingester.
// Callback将ingester传递给target，并且需要传递给ingester的key的indexes
//
// Not implemented as a method on Ring so we can test separately.
// 不实现为Ring的一个方法，因此我们可以单独进行测试
func DoBatch(ctx context.Context, r ReadRing, keys []uint32, callback func(IngesterDesc, []int) error) error {
	// 每个key按顺序对应replicationSets中的一个replicationSet
	replicationSets, err := r.BatchGet(keys, Write)
	if err != nil {
		return err
	}

	itemTrackers := make([]itemTracker, len(keys))
	ingesters := map[string]ingester{}
	for i, replicationSet := range replicationSets {
		// 最小的成功数是ingester的数目减去能够容忍的最大错误数
		itemTrackers[i].minSuccess = len(replicationSet.Ingesters) - replicationSet.MaxErrors
		// maxFailures就是能容忍的最大的错误数
		itemTrackers[i].maxFailures = replicationSet.MaxErrors

		// 遍历某个item的所有ingester
		for _, desc := range replicationSet.Ingesters {
			curr := ingesters[desc.Addr]
			ingesters[desc.Addr] = ingester{
				// 将itemTracker和对应的索引封装进ingester中
				desc:         desc,
				itemTrackers: append(curr.itemTrackers, &itemTrackers[i]),
				indexes:      append(curr.indexes, i),
			}
		}
	}

	tracker := batchTracker{
		// rpcsPending记录当前正在发送的keys的数目
		rpcsPending: int32(len(itemTrackers)),
		done:        make(chan struct{}),
		err:         make(chan error),
	}

	for _, i := range ingesters {
		go func(i ingester) {
			// 遍历ingesters，执行回调函数
			// 即某个ingester需要接收indexes所指定的item
			err := callback(i.desc, i.indexes)
			tracker.record(i.itemTrackers, err)
		}(i)
	}

	select {
	case err := <-tracker.err:
		return err
	case <-tracker.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *batchTracker) record(sampleTrackers []*itemTracker, err error) {
	// If we succeed, decrement each sample's pending count by one.  If we reach
	// the required number of successful puts on this sample, then decrement the
	// number of pending samples by one.  If we successfully push all samples to
	// min success ingesters, wake up the waiting rpc so it can return early.
	// Similarly, track the number of errors, and if it exceeds maxFailures
	// shortcut the waiting rpc.
	//
	// The use of atomic increments here guarantees only a single sendSamples
	// goroutine will write to either channel.
	for i := range sampleTrackers {
		if err != nil {
			if atomic.AddInt32(&sampleTrackers[i].failed, 1) <= int32(sampleTrackers[i].maxFailures) {
				continue
			}
			if atomic.AddInt32(&b.rpcsFailed, 1) == 1 {
				b.err <- err
			}
		} else {
			if atomic.AddInt32(&sampleTrackers[i].succeeded, 1) != int32(sampleTrackers[i].minSuccess) {
				continue
			}
			// 当有key的成功发送的数目达到minSuccess时，减小rpcsPending，当减小到0时，说明
			if atomic.AddInt32(&b.rpcsPending, -1) == 0 {
				b.done <- struct{}{}
			}
		}
	}
}
