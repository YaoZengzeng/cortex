package ring

import (
	"math/rand"
	"sort"
	"time"
)

// GenerateTokens make numTokens random tokens, none of which clash
// with takenTokens.  Assumes takenTokens is sorted.
// GenerateTokens产生numTokens个随机的tokens，它们中的任何一个都不和takenTokens冲突
// 假设takenTokens已经排过序了
func GenerateTokens(numTokens int, takenTokens []uint32) []uint32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := []uint32{}
	for i := 0; i < numTokens; {
		candidate := r.Uint32()
		j := sort.Search(len(takenTokens), func(i int) bool {
			return takenTokens[i] >= candidate
		})
		if j < len(takenTokens) && takenTokens[j] == candidate {
			continue
		}
		tokens = append(tokens, candidate)
		i++
	}
	return tokens
}

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }
