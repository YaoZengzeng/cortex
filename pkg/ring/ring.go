package ring

// Based on https://raw.githubusercontent.com/stathat/consistent/master/consistent.go

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	unhealthy = "Unhealthy"

	// ConsulKey is the key under which we store the ring in consul.
	ConsulKey = "ring"
)

// ReadRing represents the read inferface to the ring.
// ReadRing代表了对于ring的read interface
type ReadRing interface {
	prometheus.Collector

	Get(key uint32, op Operation) (ReplicationSet, error)
	BatchGet(keys []uint32, op Operation) ([]ReplicationSet, error)
	GetAll() (ReplicationSet, error)
	ReplicationFactor() int
}

// Operation can be Read or Write
type Operation int

// Values for Operation
const (
	Read Operation = iota
	Write
	// 特殊的值，用来询问健康
	Reporting // Special value for inquiring about health
)

type uint32s []uint32

func (x uint32s) Len() int           { return len(x) }
func (x uint32s) Less(i, j int) bool { return x[i] < x[j] }
func (x uint32s) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// ErrEmptyRing is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyRing = errors.New("empty ring")

// Config for a Ring
type Config struct {
	KVStore           kv.Config     `yaml:"kvstore,omitempty"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout,omitempty"`
	ReplicationFactor int           `yaml:"replication_factor,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.KVStore.RegisterFlagsWithPrefix(prefix, f)

	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes.")
	f.IntVar(&cfg.ReplicationFactor, prefix+"distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
}

// Ring holds the information about the members of the consistent hash ring.
// Ring包含了consistent hash ring中的成员的信息
type Ring struct {
	name     string
	cfg      Config
	KVClient kv.Client
	done     chan struct{}
	quit     context.CancelFunc

	mtx      sync.RWMutex
	ringDesc *Desc

	memberOwnershipDesc *prometheus.Desc
	numMembersDesc      *prometheus.Desc
	totalTokensDesc     *prometheus.Desc
	numTokensDesc       *prometheus.Desc
}

// New creates a new Ring
// New创建一个新的Ring
func New(cfg Config, name string) (*Ring, error) {
	if cfg.ReplicationFactor <= 0 {
		// ReplicationFactor必须大于0
		return nil, fmt.Errorf("ReplicationFactor must be greater than zero: %d", cfg.ReplicationFactor)
	}
	codec := codec.Proto{Factory: ProtoDescFactory}
	// 创建KV client
	store, err := kv.NewClient(cfg.KVStore, codec)
	if err != nil {
		return nil, err
	}

	r := &Ring{
		name:     name,
		cfg:      cfg,
		KVClient: store,
		done:     make(chan struct{}),
		ringDesc: &Desc{},
		memberOwnershipDesc: prometheus.NewDesc(
			"cortex_ring_member_ownership_percent",
			"The percent ownership of the ring by member",
			[]string{"member", "name"}, nil,
		),
		numMembersDesc: prometheus.NewDesc(
			"cortex_ring_members",
			"Number of members in the ring",
			[]string{"state", "name"}, nil,
		),
		totalTokensDesc: prometheus.NewDesc(
			"cortex_ring_tokens_total",
			"Number of tokens in the ring",
			[]string{"name"}, nil,
		),
		numTokensDesc: prometheus.NewDesc(
			"cortex_ring_tokens_owned",
			"The number of tokens in the ring owned by the member",
			[]string{"member", "name"}, nil,
		),
	}
	var ctx context.Context
	ctx, r.quit = context.WithCancel(context.Background())
	go r.loop(ctx)
	return r, nil
}

// Stop the distributor.
func (r *Ring) Stop() {
	r.quit()
	<-r.done
}

func (r *Ring) loop(ctx context.Context) {
	defer close(r.done)
	// 对ConsulKey进行持续地监听
	r.KVClient.WatchKey(ctx, ConsulKey, func(value interface{}) bool {
		if value == nil {
			level.Info(util.Logger).Log("msg", "ring doesn't exist in consul yet")
			return true
		}

		// 一旦ring发生变更，就更新ringDesc
		ringDesc := value.(*Desc)
		// 聚合tokens
		ringDesc.Tokens = migrateRing(ringDesc)
		r.mtx.Lock()
		defer r.mtx.Unlock()
		r.ringDesc = ringDesc
		return true
	})
}

// migrateRing will denormalise the ring's tokens if stored in normal form.
func migrateRing(desc *Desc) []TokenDesc {
	numTokens := len(desc.Tokens)
	for _, ing := range desc.Ingesters {
		// 获取ingesters的tokens的总数
		numTokens += len(ing.Tokens)
	}
	tokens := make([]TokenDesc, len(desc.Tokens), numTokens)
	// 首先拷贝已有的tokens
	copy(tokens, desc.Tokens)
	for key, ing := range desc.Ingesters {
		for _, token := range ing.Tokens {
			// 再扩展ingester中的tokens
			tokens = append(tokens, TokenDesc{
				Token:    token,
				Ingester: key,
			})
		}
	}
	sort.Sort(ByToken(tokens))
	return tokens
}

// Get returns n (or more) ingesters which form the replicas for the given key.
func (r *Ring) Get(key uint32, op Operation) (ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return r.getInternal(key, op)
}

// BatchGet returns ReplicationFactor (or more) ingesters which form the replicas
// for the given keys. The order of the result matches the order of the input.
// BatchGet返回ReplicationFactor（或者更多）个ingesters，它为给定的keys构建replicas
// 结果的顺序和输入的顺序匹配
func (r *Ring) BatchGet(keys []uint32, op Operation) ([]ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	result := make([]ReplicationSet, len(keys), len(keys))
	for i, key := range keys {
		rs, err := r.getInternal(key, op)
		if err != nil {
			return nil, err
		}
		result[i] = rs
	}
	return result, nil
}

func (r *Ring) getInternal(key uint32, op Operation) (ReplicationSet, error) {
	if r.ringDesc == nil || len(r.ringDesc.Tokens) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	var (
		// 返回至少ReplicationFactor个ingester
		n             = r.cfg.ReplicationFactor
		ingesters     = make([]IngesterDesc, 0, n)
		distinctHosts = map[string]struct{}{}
		// 找到key对应的起始的ingester
		start         = r.search(key)
		iterations    = 0
	)
	for i := start; len(distinctHosts) < n && iterations < len(r.ringDesc.Tokens); i++ {
		iterations++
		// Wrap i around in the ring.
		i %= len(r.ringDesc.Tokens)

		// We want n *distinct* ingesters.
		// 我们想要n个不同的ingesters
		token := r.ringDesc.Tokens[i]
		if _, ok := distinctHosts[token.Ingester]; ok {
			continue
		}
		// 从Ingesters中找到合适的ingesters
		distinctHosts[token.Ingester] = struct{}{}
		ingester := r.ringDesc.Ingesters[token.Ingester]

		// We do not want to Write to Ingesters that are not ACTIVE, but we do want
		// to write the extra replica somewhere.  So we increase the size of the set
		// of replicas for the key. This means we have to also increase the
		// size of the replica set for read, but we can read from Leaving ingesters,
		// so don't skip it in this case.
		// NB dead ingester will be filtered later (by replication_strategy.go).
		// 我们不想要写入不处于ACTIVE的Ingesters，但是我们想要写入额外的replica，因此我们增加这个key的set of replicas的大小
		// 这意味着我们也要增加读的replica set的大小，但是我们可以从Leaving ingesters中读取，因此在这种情况下不要跳过它
		if op == Write && ingester.State != ACTIVE {
			// 当ingester不是ACTIVE的时候却要扩展n
			n++
		} else if op == Read && (ingester.State != ACTIVE && ingester.State != LEAVING) {
			n++
		}

		ingesters = append(ingesters, ingester)
	}

	liveIngesters, maxFailure, err := r.replicationStrategy(ingesters, op)
	if err != nil {
		return ReplicationSet{}, err
	}

	return ReplicationSet{
		Ingesters: liveIngesters,
		MaxErrors: maxFailure,
	}, nil
}

// GetAll returns all available ingesters in the ring.
// GetAll返回在ring中所有可用的ingesters
func (r *Ring) GetAll() (ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.ringDesc == nil || len(r.ringDesc.Tokens) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	ingesters := make([]IngesterDesc, 0, len(r.ringDesc.Ingesters))
	// maxErros为一半的ReplicationFactor
	maxErrors := r.cfg.ReplicationFactor / 2

	for _, ingester := range r.ringDesc.Ingesters {
		if !r.IsHealthy(&ingester, Read) {
			// 有一个unhealthy的，就将maxErros减1
			maxErrors--
			continue
		}
		ingesters = append(ingesters, ingester)
	}

	if maxErrors < 0 {
		return ReplicationSet{}, fmt.Errorf("too many failed ingesters")
	}

	return ReplicationSet{
		Ingesters: ingesters,
		MaxErrors: maxErrors,
	}, nil
}

func (r *Ring) search(key uint32) int {
	i := sort.Search(len(r.ringDesc.Tokens), func(x int) bool {
		// 找到大于key的token
		return r.ringDesc.Tokens[x].Token > key
	})
	if i >= len(r.ringDesc.Tokens) {
		// 找到相应的虚拟节点
		i = 0
	}
	return i
}

// Describe implements prometheus.Collector.
func (r *Ring) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.memberOwnershipDesc
	ch <- r.numMembersDesc
	ch <- r.totalTokensDesc
	ch <- r.numTokensDesc
}

func countTokens(ringDesc *Desc) (map[string]uint32, map[string]uint32) {
	tokens := ringDesc.Tokens

	owned := map[string]uint32{}
	numTokens := map[string]uint32{}
	for i, token := range tokens {
		var diff uint32
		if i+1 == len(tokens) {
			diff = (math.MaxUint32 - token.Token) + tokens[0].Token
		} else {
			diff = tokens[i+1].Token - token.Token
		}
		numTokens[token.Ingester] = numTokens[token.Ingester] + 1
		owned[token.Ingester] = owned[token.Ingester] + diff
	}

	for id := range ringDesc.Ingesters {
		if _, ok := owned[id]; !ok {
			owned[id] = 0
			numTokens[id] = 0
		}
	}

	return numTokens, owned
}

// Collect implements prometheus.Collector.
func (r *Ring) Collect(ch chan<- prometheus.Metric) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	numTokens, ownedRange := countTokens(r.ringDesc)
	for id, totalOwned := range ownedRange {
		ch <- prometheus.MustNewConstMetric(
			r.memberOwnershipDesc,
			prometheus.GaugeValue,
			float64(totalOwned)/float64(math.MaxUint32),
			id,
			r.name,
		)
		ch <- prometheus.MustNewConstMetric(
			r.numTokensDesc,
			prometheus.GaugeValue,
			float64(numTokens[id]),
			id,
			r.name,
		)
	}

	// Initialised to zero so we emit zero-metrics (instead of not emitting anything)
	byState := map[string]int{
		unhealthy:        0,
		ACTIVE.String():  0,
		LEAVING.String(): 0,
		PENDING.String(): 0,
		JOINING.String(): 0,
	}
	for _, ingester := range r.ringDesc.Ingesters {
		if !r.IsHealthy(&ingester, Reporting) {
			byState[unhealthy]++
		} else {
			byState[ingester.State.String()]++
		}
	}

	for state, count := range byState {
		ch <- prometheus.MustNewConstMetric(
			r.numMembersDesc,
			prometheus.GaugeValue,
			float64(count),
			state,
			r.name,
		)
	}
	ch <- prometheus.MustNewConstMetric(
		r.totalTokensDesc,
		prometheus.GaugeValue,
		float64(len(r.ringDesc.Tokens)),
		r.name,
	)
}
