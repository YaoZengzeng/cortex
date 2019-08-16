package ring

import (
	"fmt"
	"time"
)

// replicationStrategy decides, given the set of ingesters eligible for a key,
// which ingesters you will try and write to and how many failures you will
// tolerate.
// - Filters out dead ingesters so the one doesn't even try to write to them.
// - Checks there is enough ingesters for an operation to succeed.
// replicationStrategy决定，给定一系列适合key的一系列ingesters，哪些ingesters你会尝试并且写入，以及你能忍受多少failures
// - 过滤死的ingesters，这样就不会尝试向它们写入
// - 检查是否有足够的ingesters保证操作成功
func (r *Ring) replicationStrategy(ingesters []IngesterDesc, op Operation) (
	liveIngesters []IngesterDesc, maxFailure int, err error,
) {
	// We need a response from a quorum of ingesters, which is n/2 + 1.  In the
	// case of a node joining/leaving, the actual replica set might be bigger
	// than the replication factor, so use the bigger or the two.
	// 我们需要从quorum of ingesters中获取response，即n/2 + 1，在node加入以及离开的过程中，
	// 真正的replica set可能比replication factor要大，因此使用大的一个
	replicationFactor := r.cfg.ReplicationFactor
	if len(ingesters) > replicationFactor {
		// 将replicationFactor更新为当前的ingester的数目
		replicationFactor = len(ingesters)
	}
	minSuccess := (replicationFactor / 2) + 1
	maxFailure = replicationFactor - minSuccess

	// Skip those that have not heartbeated in a while. NB these are still
	// included in the calculation of minSuccess, so if too many failed ingesters
	// will cause the whole write to fail.
	// 跳过那些很久没有心跳的ingester，但是它们依然包含在minSuccess中，因此如果失败了太多的ingesters，会导致整个写入的失败
	liveIngesters = make([]IngesterDesc, 0, len(ingesters))
	for _, ingester := range ingesters {
		if r.IsHealthy(&ingester, op) {
			// 过滤出liveIngesters	
			liveIngesters = append(liveIngesters, ingester)
		} else {
			maxFailure--
		}
	}

	// This is just a shortcut - if there are not minSuccess available ingesters,
	// after filtering out dead ones, don't even bother trying.
	// 如果没有minSuccess个可用的ingesters，在过滤了死的ingester之后，则直接退出
	if maxFailure < 0 || len(liveIngesters) < minSuccess {
		err = fmt.Errorf("at least %d live ingesters required, could only find %d",
			minSuccess, len(liveIngesters))
		return
	}

	return
}

// IsHealthy checks whether an ingester appears to be alive and heartbeating
// IsHealthy检查是否ingester依然活跃并且进行heartbeating
func (r *Ring) IsHealthy(ingester *IngesterDesc, op Operation) bool {
	if op == Write && ingester.State != ACTIVE {
		// 如果是写并且状态不为ACTIVE就为false
		return false
	} else if op == Read && ingester.State == JOINING {
		// 如果为读，状态为JOINING就返回false
		return false
	}
	// 如果超过HeartbeatTimeout没有心跳，则也被认为是不健康的
	return time.Now().Sub(time.Unix(ingester.Timestamp, 0)) <= r.cfg.HeartbeatTimeout
}

// ReplicationFactor of the ring.
func (r *Ring) ReplicationFactor() int {
	return r.cfg.ReplicationFactor
}
