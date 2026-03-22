// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ssb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	refs "github.com/ssbc/go-ssb-refs"
)

// FeedSeq pairs a feed reference with the highest observed sequence number.
type FeedSeq struct {
	Feed refs.FeedRef
	Seq  int64
}

// FeedAdvance describes how a feed changed between two frontiers.
type FeedAdvance struct {
	Feed   refs.FeedRef
	OldSeq int64 // -1 if feed is new in the later frontier
	NewSeq int64 // -1 if feed was dropped in the later frontier
}

// Frontier is a vector clock over SSB feeds.
// Each entry maps a feed to the highest sequence number observed.
// Entries are kept sorted by feed ref string for deterministic comparison.
type Frontier struct {
	feeds []FeedSeq
}

// NewFrontier creates a Frontier from the given entries.
// Duplicate feeds use the highest sequence number.
func NewFrontier(entries ...FeedSeq) Frontier {
	if len(entries) == 0 {
		return Frontier{}
	}

	// Deduplicate: keep max seq per feed
	seen := make(map[string]int) // feed.String() → index in result
	deduped := make([]FeedSeq, 0, len(entries))
	for _, e := range entries {
		key := e.Feed.String()
		if idx, ok := seen[key]; ok {
			if e.Seq > deduped[idx].Seq {
				deduped[idx].Seq = e.Seq
			}
		} else {
			seen[key] = len(deduped)
			deduped = append(deduped, e)
		}
	}

	sortFeedSeqs(deduped)
	return Frontier{feeds: deduped}
}

// Set returns a new Frontier with the given feed set to seq.
// If the feed already exists, its sequence is updated.
func (f Frontier) Set(feed refs.FeedRef, seq int64) Frontier {
	entries := make([]FeedSeq, len(f.feeds), len(f.feeds)+1)
	copy(entries, f.feeds)

	key := feed.String()
	for i, e := range entries {
		if e.Feed.String() == key {
			entries[i].Seq = seq
			return Frontier{feeds: entries}
		}
	}

	entries = append(entries, FeedSeq{Feed: feed, Seq: seq})
	sortFeedSeqs(entries)
	return Frontier{feeds: entries}
}

// Len returns the number of feeds in this frontier.
func (f Frontier) Len() int {
	return len(f.feeds)
}

// Feeds returns a copy of all feed-seq entries.
func (f Frontier) Feeds() []FeedSeq {
	out := make([]FeedSeq, len(f.feeds))
	copy(out, f.feeds)
	return out
}

// Seq returns the sequence for a given feed, or -1 if not present.
func (f Frontier) Seq(feed refs.FeedRef) int64 {
	key := feed.String()
	for _, e := range f.feeds {
		if e.Feed.String() == key {
			return e.Seq
		}
	}
	return -1
}

// Equal returns true if both frontiers contain the same feeds with the same sequences.
func (f Frontier) Equal(other Frontier) bool {
	if len(f.feeds) != len(other.feeds) {
		return false
	}
	for i, e := range f.feeds {
		o := other.feeds[i]
		if e.Seq != o.Seq || e.Feed.String() != o.Feed.String() {
			return false
		}
	}
	return true
}

// HappenedBefore returns true if f ≤ other in the vector clock partial order.
// This means: every feed in f is also in other, and its seq in f is ≤ its seq in other.
// An empty frontier happened before everything.
func (f Frontier) HappenedBefore(other Frontier) bool {
	if len(f.feeds) == 0 {
		return true
	}

	// Build a lookup for other's feeds
	otherMap := make(map[string]int64, len(other.feeds))
	for _, e := range other.feeds {
		otherMap[e.Feed.String()] = e.Seq
	}

	for _, e := range f.feeds {
		otherSeq, ok := otherMap[e.Feed.String()]
		if !ok {
			// f has a feed that other doesn't → not ≤
			return false
		}
		if e.Seq > otherSeq {
			return false
		}
	}
	return true
}

// Concurrent returns true if neither f ≤ other nor other ≤ f.
// This means the two frontiers represent incomparable worldviews.
func (f Frontier) Concurrent(other Frontier) bool {
	return !f.HappenedBefore(other) && !other.HappenedBefore(f)
}

// Diff computes the differences between f and other.
// For each feed that differs, it returns a FeedAdvance with the old and new sequence.
// Feeds present in other but not in f have OldSeq = -1.
// Feeds present in f but not in other have NewSeq = -1.
func (f Frontier) Diff(other Frontier) []FeedAdvance {
	fMap := make(map[string]FeedSeq, len(f.feeds))
	for _, e := range f.feeds {
		fMap[e.Feed.String()] = e
	}

	oMap := make(map[string]FeedSeq, len(other.feeds))
	for _, e := range other.feeds {
		oMap[e.Feed.String()] = e
	}

	var advances []FeedAdvance

	// Feeds in other (new or advanced)
	for key, oe := range oMap {
		fe, ok := fMap[key]
		if !ok {
			advances = append(advances, FeedAdvance{
				Feed:   oe.Feed,
				OldSeq: -1,
				NewSeq: oe.Seq,
			})
		} else if fe.Seq != oe.Seq {
			advances = append(advances, FeedAdvance{
				Feed:   oe.Feed,
				OldSeq: fe.Seq,
				NewSeq: oe.Seq,
			})
		}
	}

	// Feeds dropped (in f but not in other)
	for key, fe := range fMap {
		if _, ok := oMap[key]; !ok {
			advances = append(advances, FeedAdvance{
				Feed:   fe.Feed,
				OldSeq: fe.Seq,
				NewSeq: -1,
			})
		}
	}

	// Sort for deterministic output
	sort.Slice(advances, func(i, j int) bool {
		return advances[i].Feed.String() < advances[j].Feed.String()
	})

	return advances
}

// Merge returns the join (least upper bound) of f and other.
// For each feed present in either frontier, it takes the maximum sequence.
func (f Frontier) Merge(other Frontier) Frontier {
	merged := make(map[string]FeedSeq, len(f.feeds)+len(other.feeds))

	for _, e := range f.feeds {
		merged[e.Feed.String()] = e
	}
	for _, e := range other.feeds {
		key := e.Feed.String()
		if existing, ok := merged[key]; ok {
			if e.Seq > existing.Seq {
				merged[key] = e
			}
		} else {
			merged[key] = e
		}
	}

	result := make([]FeedSeq, 0, len(merged))
	for _, e := range merged {
		result = append(result, e)
	}
	sortFeedSeqs(result)
	return Frontier{feeds: result}
}

// FrontierFromNetworkFrontier converts an EBT NetworkFrontier into a Frontier,
// stripping replication control bits and keeping only feed→seq mappings.
// Feeds with Replicate=false (seq==-1 semantics) are skipped.
func FrontierFromNetworkFrontier(nf NetworkFrontier) (Frontier, error) {
	entries := make([]FeedSeq, 0, len(nf))
	for feedStr, note := range nf {
		if !note.Replicate {
			continue
		}
		feed, err := refs.ParseFeedRef(feedStr)
		if err != nil {
			return Frontier{}, fmt.Errorf("frontier: invalid feed ref %q: %w", feedStr, err)
		}
		entries = append(entries, FeedSeq{Feed: feed, Seq: note.Seq})
	}
	sortFeedSeqs(entries)
	return Frontier{feeds: entries}, nil
}

// String returns a human-readable representation of the frontier.
func (f Frontier) String() string {
	if len(f.feeds) == 0 {
		return "Frontier{}"
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Frontier{%d feeds", len(f.feeds))
	if len(f.feeds) <= 5 {
		buf.WriteString(": ")
		for i, e := range f.feeds {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%s:%d", e.Feed.ShortSigil(), e.Seq)
		}
	}
	buf.WriteByte('}')
	return buf.String()
}

// Marshal encodes the frontier into a compact binary format.
//
// Wire format:
//
//	[count: uvarint]
//	for each feed (sorted by ref string):
//	  [algo_len: uvarint] [algo: bytes]
//	  [pubkey_len: uvarint] [pubkey: bytes]
//	  [seq: uvarint]
//
// This supports all feed algorithms, not just ed25519.
func (f Frontier) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	// Write count
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(f.feeds)))
	buf.Write(tmp[:n])

	for _, e := range f.feeds {
		algo := string(e.Feed.Algo())
		n = binary.PutUvarint(tmp[:], uint64(len(algo)))
		buf.Write(tmp[:n])
		buf.WriteString(algo)

		pubkey := e.Feed.PubKey()
		n = binary.PutUvarint(tmp[:], uint64(len(pubkey)))
		buf.Write(tmp[:n])
		buf.Write(pubkey)

		n = binary.PutUvarint(tmp[:], uint64(e.Seq))
		buf.Write(tmp[:n])
	}

	return buf.Bytes(), nil
}

// UnmarshalFrontier decodes a frontier from the compact binary format produced by Marshal.
func UnmarshalFrontier(data []byte) (Frontier, error) {
	r := bytes.NewReader(data)

	count, err := binary.ReadUvarint(r)
	if err != nil {
		return Frontier{}, fmt.Errorf("frontier unmarshal: reading count: %w", err)
	}

	if count > 100000 {
		return Frontier{}, fmt.Errorf("frontier unmarshal: count too large: %d", count)
	}

	feeds := make([]FeedSeq, 0, count)
	for i := uint64(0); i < count; i++ {
		algoLen, err := binary.ReadUvarint(r)
		if err != nil {
			return Frontier{}, fmt.Errorf("frontier unmarshal: reading algo len at %d: %w", i, err)
		}
		algoBytes := make([]byte, algoLen)
		if _, err := r.Read(algoBytes); err != nil {
			return Frontier{}, fmt.Errorf("frontier unmarshal: reading algo at %d: %w", i, err)
		}

		keyLen, err := binary.ReadUvarint(r)
		if err != nil {
			return Frontier{}, fmt.Errorf("frontier unmarshal: reading key len at %d: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := r.Read(keyBytes); err != nil {
			return Frontier{}, fmt.Errorf("frontier unmarshal: reading key at %d: %w", i, err)
		}

		seq, err := binary.ReadUvarint(r)
		if err != nil {
			return Frontier{}, fmt.Errorf("frontier unmarshal: reading seq at %d: %w", i, err)
		}

		feed, err := refs.NewFeedRefFromBytes(keyBytes, refs.RefAlgo(algoBytes))
		if err != nil {
			return Frontier{}, fmt.Errorf("frontier unmarshal: creating feed ref at %d: %w", i, err)
		}

		feeds = append(feeds, FeedSeq{Feed: feed, Seq: int64(seq)})
	}

	sortFeedSeqs(feeds)
	return Frontier{feeds: feeds}, nil
}

func sortFeedSeqs(s []FeedSeq) {
	sort.Slice(s, func(i, j int) bool {
		return s[i].Feed.String() < s[j].Feed.String()
	})
}
