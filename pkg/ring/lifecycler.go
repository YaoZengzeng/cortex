package ring

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	consulHeartbeats = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_member_consul_heartbeats_total",
		Help: "The total number of heartbeats sent to consul.",
	}, []string{"name"})
	tokensOwned = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_member_ring_tokens_owned",
		Help: "The number of tokens owned in the ring.",
	}, []string{"name"})
	tokensToOwn = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_member_ring_tokens_to_own",
		Help: "The number of tokens to own in the ring.",
	}, []string{"name"})
	shutdownDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_shutdown_duration_seconds",
		Help:    "Duration (in seconds) of cortex shutdown procedure (ie transfer or flush).",
		Buckets: prometheus.ExponentialBuckets(10, 2, 8), // Biggest bucket is 10*2^(9-1) = 2560, or 42 mins.
	}, []string{"op", "status", "name"})
)

// LifecyclerConfig is the config to build a Lifecycler.
type LifecyclerConfig struct {
	RingConfig Config `yaml:"ring,omitempty"`

	// Config for the ingester lifecycle control
	// ingester监听的端口
	ListenPort       *int
	NumTokens        int           `yaml:"num_tokens,omitempty"`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period,omitempty"`
	JoinAfter        time.Duration `yaml:"join_after,omitempty"`
	MinReadyDuration time.Duration `yaml:"min_ready_duration,omitempty"`
	ClaimOnRollout   bool          `yaml:"claim_on_rollout,omitempty"`
	NormaliseTokens  bool          `yaml:"normalise_tokens,omitempty"`
	InfNames         []string      `yaml:"interface_names"`
	FinalSleep       time.Duration `yaml:"final_sleep"`

	// For testing, you can override the address and ID of this ingester
	Addr           string `yaml:"address"`
	Port           int
	ID             string
	SkipUnregister bool
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *LifecyclerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet.
func (cfg *LifecyclerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RingConfig.RegisterFlagsWithPrefix(prefix, f)

	// In order to keep backwards compatibility all of these need to be prefixed
	// with "ingester."
	// 所有都要加"ingester."的前缀
	if prefix == "" {
		prefix = "ingester."
	}

	// 每个ingester的token的数目
	f.IntVar(&cfg.NumTokens, prefix+"num-tokens", 128, "Number of tokens for each ingester.")
	// 对consul进行heartbeat的周期
	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 5*time.Second, "Period at which to heartbeat to consul.")
	f.DurationVar(&cfg.JoinAfter, prefix+"join-after", 0*time.Second, "Period to wait for a claim from another member; will join automatically after this.")
	// 至少一分钟后才会变为ready，防止ingester退出以及更新ring产生冲突
	f.DurationVar(&cfg.MinReadyDuration, prefix+"min-ready-duration", 1*time.Minute, "Minimum duration to wait before becoming ready. This is to work around race conditions with ingesters exiting and updating the ring.")
	// 在退出的时候将chunk发送到PENDING ingesters
	f.BoolVar(&cfg.ClaimOnRollout, prefix+"claim-on-rollout", false, "Send chunks to PENDING ingesters on exit.")
	f.BoolVar(&cfg.NormaliseTokens, prefix+"normalise-tokens", false, "Store tokens in a normalised fashion to reduce allocations.")
	f.DurationVar(&cfg.FinalSleep, prefix+"final-sleep", 30*time.Second, "Duration to sleep for before exiting, to ensure metrics are scraped.")

	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	cfg.InfNames = []string{"eth0", "en0"}
	f.Var((*flagext.Strings)(&cfg.InfNames), prefix+"lifecycler.interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.Addr, prefix+"lifecycler.addr", "", "IP address to advertise in consul.")
	f.IntVar(&cfg.Port, prefix+"lifecycler.port", 0, "port to advertise in consul (defaults to server.grpc-listen-port).")
	// ID就是hostname
	f.StringVar(&cfg.ID, prefix+"lifecycler.ID", hostname, "ID to register into consul.")
}

// FlushTransferer controls the shutdown of an ingester.
type FlushTransferer interface {
	StopIncomingRequests()
	Flush()
	TransferOut(ctx context.Context) error
}

// Lifecycler is responsible for managing the lifecycle of entries in the ring.
// Lifecycler负责管理ring中的entries的生命周期
type Lifecycler struct {
	cfg             LifecyclerConfig
	flushTransferer FlushTransferer
	KVStore         kv.Client

	// Controls the lifecycle of the ingester
	// 控制ingester的生命周期
	quit      chan struct{}
	done      sync.WaitGroup
	actorChan chan func()

	// These values are initialised at startup, and never change
	ID       string
	Addr     string
	RingName string

	// We need to remember the ingester state just in case consul goes away and comes
	// back empty.  And it changes during lifecycle of ingester.
	// 我们需要记住ingester的状态以防consul消失并且变为空，它在ingester的生命周期之内改变
	stateMtx sync.Mutex
	state    IngesterState
	tokens   []uint32

	// Controls the ready-reporting
	readyLock sync.Mutex
	startTime time.Time
	ready     bool
}

// NewLifecycler makes and starts a new Lifecycler.
// NewLifecycler创建并且启动一个Lifecycler
func NewLifecycler(cfg LifecyclerConfig, flushTransferer FlushTransferer, name string) (*Lifecycler, error) {
	addr := cfg.Addr
	if addr == "" {
		var err error
		addr, err = util.GetFirstAddressOf(cfg.InfNames)
		if err != nil {
			return nil, err
		}
	}
	port := cfg.Port
	if port == 0 {
		port = *cfg.ListenPort
	}
	codec := GetCodec()
	store, err := kv.NewClient(cfg.RingConfig.KVStore, codec)
	if err != nil {
		return nil, err
	}

	l := &Lifecycler{
		cfg:             cfg,
		flushTransferer: flushTransferer,
		KVStore:         store,

		Addr:     fmt.Sprintf("%s:%d", addr, port),
		ID:       cfg.ID,
		RingName: name,

		quit:      make(chan struct{}),
		actorChan: make(chan func()),

		// 初始状态设置为PENDING
		state:     PENDING,
		startTime: time.Now(),
	}

	tokensToOwn.WithLabelValues(l.RingName).Set(float64(cfg.NumTokens))

	l.done.Add(1)
	// loop将ingester加入ring并且更新定期进行心跳
	go l.loop()
	return l, nil
}

// CheckReady is used to rate limit the number of ingesters that can be coming or
// going at any one time, by only returning true if all ingesters are active.
// The state latches: once we have gone ready we don't go un-ready
// CheckReady用来限制任何时候来来去去的ingesters的数目，只有在所有ingesters都活跃的时候返回true
// 状态锁定：一旦我们ready了，就不会再un-ready
func (i *Lifecycler) CheckReady(ctx context.Context) error {
	i.readyLock.Lock()
	defer i.readyLock.Unlock()

	if i.ready {
		return nil
	}

	// Ingester always take at least minReadyDuration to become ready to work
	// around race conditions with ingesters exiting and updating the ring
	// Ingester总是花至少minReadyDuration来准备ready，用来避免ingesters退出以及更新ring的冲突
	if time.Now().Sub(i.startTime) < i.cfg.MinReadyDuration {
		return fmt.Errorf("waiting for %v after startup", i.cfg.MinReadyDuration)
	}

	ringDesc, err := i.KVStore.Get(ctx, ConsulKey)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error talking to consul", "err", err)
		return fmt.Errorf("error talking to consul: %s", err)
	}

	if len(i.getTokens()) == 0 {
		// ingester没有token，则退出
		return fmt.Errorf("this ingester owns no tokens")
	}
	// 当所有ingester都active和healthy的时候才返回nil
	if err := ringDesc.(*Desc).Ready(i.cfg.RingConfig.HeartbeatTimeout); err != nil {
		return err
	}

	i.ready = true
	return nil
}

// GetState returns the state of this ingester.
func (i *Lifecycler) GetState() IngesterState {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	return i.state
}

func (i *Lifecycler) setState(state IngesterState) {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	i.state = state
}

// ChangeState of the ingester, for use off of the loop() goroutine.
func (i *Lifecycler) ChangeState(ctx context.Context, state IngesterState) error {
	err := make(chan error)
	i.actorChan <- func() {
		err <- i.changeState(ctx, state)
	}
	return <-err
}

func (i *Lifecycler) getTokens() []uint32 {
	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	return i.tokens
}

func (i *Lifecycler) setTokens(tokens []uint32) {
	tokensOwned.WithLabelValues(i.RingName).Set(float64(len(tokens)))

	i.stateMtx.Lock()
	defer i.stateMtx.Unlock()
	i.tokens = tokens
}

// ClaimTokensFor takes all the tokens for the supplied ingester and assigns them to this ingester.
func (i *Lifecycler) ClaimTokensFor(ctx context.Context, ingesterID string) error {
	err := make(chan error)

	i.actorChan <- func() {
		var tokens []uint32

		claimTokens := func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc, ok := in.(*Desc)
			if !ok || ringDesc == nil {
				return nil, false, fmt.Errorf("Cannot claim tokens in an empty ring")
			}

			tokens = ringDesc.ClaimTokens(ingesterID, i.ID, i.cfg.NormaliseTokens)
			return ringDesc, true, nil
		}

		if err := i.KVStore.CAS(ctx, ConsulKey, claimTokens); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to write to consul", "err", err)
		}

		i.setTokens(tokens)
		err <- nil
	}

	return <-err
}

// Shutdown the lifecycle.  It will:
// - send chunks to another ingester, if it can.
// - otherwise, flush chunks to the chunk store.
// - remove config from Consul.
// - block until we've successfully shutdown.
// 停止lifecycle，它会：
// - 将chunks发送到另一个ingester，如果可以的话
// - 否则flush chunks到chunk store
// - 从Consul中移除配置
// - 阻塞直到我们成功退出
func (i *Lifecycler) Shutdown() {
	// This will prevent us accepting any more samples
	// 不再接受新的samples
	i.flushTransferer.StopIncomingRequests()

	// closing i.quit triggers loop() to exit, which in turn will trigger
	// the removal of our tokens etc
	// 关闭i.quit会触发loop()退出，之后又会触发我们的token的移除
	close(i.quit)
	i.done.Wait()
}

func (i *Lifecycler) loop() {
	defer func() {
		level.Info(util.Logger).Log("msg", "member.loop() exited gracefully")
		i.done.Done()
	}()

	// First, see if we exist in the cluster, update our state to match if we do,
	// and add ourselves (without tokens) if we don't.
	// 首先，检查我们是否在cluster中，如果在的话，就更新状态，如果不在的话，就加入（没有token）
	if err := i.initRing(context.Background()); err != nil {
		level.Error(util.Logger).Log("msg", "failed to join consul", "err", err)
		os.Exit(1)
	}

	// We do various period tasks
	autoJoinAfter := time.After(i.cfg.JoinAfter)

	heartbeatTicker := time.NewTicker(i.cfg.HeartbeatPeriod)
	defer heartbeatTicker.Stop()

loop:
	for {
		select {
		case <-autoJoinAfter:
			level.Debug(util.Logger).Log("msg", "JoinAfter expired")
			// Will only fire once, after auto join timeout.  If we haven't entered "JOINING" state,
			// then pick some tokens and enter ACTIVE state.
			// 之后执行一次，在auto join超时之后，如果我们没有进入"JOINING"状态，那么拿一些tokens并且进入ACTIVE状态
			if i.GetState() == PENDING {
				level.Info(util.Logger).Log("msg", "auto-joining cluster after timeout")
				if err := i.autoJoin(context.Background()); err != nil {
					level.Error(util.Logger).Log("msg", "failed to pick tokens in consul", "err", err)
					os.Exit(1)
				}
			}

		case <-heartbeatTicker.C:
			// 每隔5s进行一次
			consulHeartbeats.WithLabelValues(i.RingName).Inc()
			if err := i.updateConsul(context.Background()); err != nil {
				level.Error(util.Logger).Log("msg", "failed to write to consul, sleeping", "err", err)
			}

		case f := <-i.actorChan:
			f()

		case <-i.quit:
			// 正常退出的话，会通过这里
			break loop
		}
	}

	// Mark ourselved as Leaving so no more samples are send to us.
	// 将自己标记为Leaving，因此不会有samples再发送过来
	i.changeState(context.Background(), LEAVING)

	// Do the transferring / flushing on a background goroutine so we can continue
	// to heartbeat to consul.
	// 在后台的goroutine做tranferring/flushing，因此我们可以持续地对consul做心跳
	done := make(chan struct{})
	go func() {
		i.processShutdown(context.Background())
		close(done)
	}()

heartbeatLoop:
	for {
		select {
		case <-heartbeatTicker.C:
			consulHeartbeats.WithLabelValues(i.RingName).Inc()
			// 在transferring/flushing期间一直对consul发起心跳
			if err := i.updateConsul(context.Background()); err != nil {
				level.Error(util.Logger).Log("msg", "failed to write to consul, sleeping", "err", err)
			}

		case <-done:
			break heartbeatLoop
		}
	}

	if !i.cfg.SkipUnregister {
		// 从consul中移除ingester
		if err := i.unregister(context.Background()); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to unregister from consul", "err", err)
			os.Exit(1)
		}
		level.Info(util.Logger).Log("msg", "ingester removed from consul")
	}
}

// initRing is the first thing we do when we start. It:
// - add an ingester entry to the ring
// - copies out our state and tokens if they exist
// initRing是我们在启动的时候第一件要做的事情，它在ring中增加一个ingester entry，如果存在的话，拷贝出状态以及tokens
func (i *Lifecycler) initRing(ctx context.Context) error {
	return i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[i.ID]
		if !ok {
			// Either we are a new ingester, or consul must have restarted
			// 我们是一个新的ingester或者consul重启了
			level.Info(util.Logger).Log("msg", "entry not found in ring, adding with no tokens")
			ringDesc.AddIngester(i.ID, i.Addr, []uint32{}, i.GetState(), i.cfg.NormaliseTokens)
			return ringDesc, true, nil
		}

		// We exist in the ring, so assume the ring is right and copy out tokens & state out of there.
		// 已经在ring中存在了，因此假设ring是正确的，因此拷贝出tokens以及状态
		i.setState(ingesterDesc.State)
		tokens, _ := ringDesc.TokensFor(i.ID)
		i.setTokens(tokens)

		level.Info(util.Logger).Log("msg", "existing entry found in ring", "state", i.GetState(), "tokens", len(tokens))
		return ringDesc, true, nil
	})
}

// autoJoin selects random tokens & moves state to ACTIVE
// autoJoin选择随机的tokens并且移动状态到ACTIVE
func (i *Lifecycler) autoJoin(ctx context.Context) error {
	return i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		// At this point, we should not have any tokens, and we should be in PENDING state.
		// 当前我们不应该有任何的tokens，并且应该处于PENDING状态
		myTokens, takenTokens := ringDesc.TokensFor(i.ID)
		if len(myTokens) > 0 {
			level.Error(util.Logger).Log("msg", "tokens already exist for this ingester - wasn't expecting any!", "num_tokens", len(myTokens))
		}

		newTokens := GenerateTokens(i.cfg.NumTokens-len(myTokens), takenTokens)
		// 直接将状态设置为ACTIVE
		i.setState(ACTIVE)
		// 加入Ingester
		ringDesc.AddIngester(i.ID, i.Addr, newTokens, i.GetState(), i.cfg.NormaliseTokens)

		tokens := append(myTokens, newTokens...)
		sort.Sort(sortableUint32(tokens))
		i.setTokens(tokens)

		return ringDesc, true, nil
	})
}

// updateConsul updates our entries in consul, heartbeating and dealing with
// consul restarts.
// updateConsul更新我们在consul里的entries，心跳用于处理consul的重启
func (i *Lifecycler) updateConsul(ctx context.Context) error {
	return i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		// 如果没有ring，就先创建一个
		if in == nil {
			ringDesc = NewDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		// 将ingester加入ring中
		ingesterDesc, ok := ringDesc.Ingesters[i.ID]
		if !ok {
			// consul must have restarted
			// consul肯定被重启了
			level.Info(util.Logger).Log("msg", "found empty ring, inserting tokens")
			ringDesc.AddIngester(i.ID, i.Addr, i.getTokens(), i.GetState(), i.cfg.NormaliseTokens)
		} else {
			// 更新ingester的状态
			ingesterDesc.Timestamp = time.Now().Unix()
			ingesterDesc.State = i.GetState()
			ingesterDesc.Addr = i.Addr
			ringDesc.Ingesters[i.ID] = ingesterDesc
		}

		return ringDesc, true, nil
	})
}

// changeState updates consul with state transitions for us.  NB this must be
// called from loop()!  Use ChangeState for calls from outside of loop().
func (i *Lifecycler) changeState(ctx context.Context, state IngesterState) error {
	currState := i.GetState()
	// Only the following state transitions can be triggered externally
	// 只有以下的状态转移才能从外部触发
	// 从PENDING转移到JOINING，从JOINING转移到PENDING，从JOINING转移到ACTIVE，从PENDING转移到ACTIVE，从ACTIVE转移到LEAVING
	if !((currState == PENDING && state == JOINING) || // triggered by TransferChunks at the beginning
		(currState == JOINING && state == PENDING) || // triggered by TransferChunks on failure
		(currState == JOINING && state == ACTIVE) || // triggered by TransferChunks on success
		(currState == PENDING && state == ACTIVE) || // triggered by autoJoin
		(currState == ACTIVE && state == LEAVING)) { // triggered by shutdown
		return fmt.Errorf("Changing ingester state from %v -> %v is disallowed", currState, state)
	}

	level.Info(util.Logger).Log("msg", "changing ingester state from", "old_state", currState, "new_state", state)
	i.setState(state)
	return i.updateConsul(ctx)
}

func (i *Lifecycler) processShutdown(ctx context.Context) {
	flushRequired := true
	if i.cfg.ClaimOnRollout {
		transferStart := time.Now()
		// 将chunk移除到另一个ingester
		if err := i.flushTransferer.TransferOut(ctx); err != nil {
			level.Error(util.Logger).Log("msg", "Failed to transfer chunks to another ingester", "err", err)
			shutdownDuration.WithLabelValues("transfer", "fail", i.RingName).Observe(time.Since(transferStart).Seconds())
		} else {
			flushRequired = false
			shutdownDuration.WithLabelValues("transfer", "success", i.RingName).Observe(time.Since(transferStart).Seconds())
		}
	}

	if flushRequired {
		flushStart := time.Now()
		i.flushTransferer.Flush()
		shutdownDuration.WithLabelValues("flush", "success", i.RingName).Observe(time.Since(flushStart).Seconds())
	}

	// Sleep so the shutdownDuration metric can be collected.
	time.Sleep(i.cfg.FinalSleep)
}

// unregister removes our entry from consul.
func (i *Lifecycler) unregister(ctx context.Context) error {
	level.Debug(util.Logger).Log("msg", "unregistering member from ring")

	return i.KVStore.CAS(ctx, ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.RemoveIngester(i.ID)
		return ringDesc, true, nil
	})
}
