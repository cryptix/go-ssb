// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multilogs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/dgraph-io/sroar"
	"github.com/keks/persist"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"

	margaret "github.com/ssbc/margaret/v2"

	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/repo"
)

const searchBatchSize = 128

// SearchIndex provides full-text search over SSB messages using Bleve.
// It follows the same incremental indexing pattern as CombinedIndex.
type SearchIndex struct {
	logger log.Logger
	index  bleve.Index

	stateFile *os.File
	mu        sync.Mutex

	onEntry func()
}

// SetOnEntry sets a callback that is invoked for each entry processed by Index().
func (idx *SearchIndex) SetOnEntry(fn func()) {
	idx.onEntry = fn
}

// NewSearchIndex opens or creates a Bleve full-text search index.
func NewSearchIndex(logger log.Logger, repoPath string) (*SearchIndex, error) {
	r := repo.New(repoPath)
	idxPath := r.GetPath(repo.PrefixMultiLog, "search")
	statePath := r.GetPath(repo.PrefixMultiLog, "search-state.json")

	os.MkdirAll(filepath.Dir(statePath), 0700)

	mode := os.O_RDWR | os.O_EXCL
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		mode |= os.O_CREATE
	}
	stateFile, err := os.OpenFile(statePath, mode, 0700)
	if err != nil {
		return nil, fmt.Errorf("search index: error opening state file: %w", err)
	}

	var idx bleve.Index
	idx, err = bleve.Open(idxPath)
	if err != nil {
		if err != bleve.ErrorIndexPathDoesNotExist {
			stateFile.Close()
			return nil, fmt.Errorf("search index: error opening index: %w", err)
		}
		idx, err = bleve.New(idxPath, buildSearchMapping())
		if err != nil {
			stateFile.Close()
			return nil, fmt.Errorf("search index: error creating index: %w", err)
		}
	}

	si := &SearchIndex{
		logger:    log.With(logger, "unit", "search-index"),
		index:     idx,
		stateFile: stateFile,
	}

	// Log what fields the stored index mapping actually has, so we can
	// detect stale/broken mappings from previous versions.
	if fields, err := idx.Fields(); err == nil {
		level.Info(si.logger).Log("event", "index-opened", "indexed_fields", fmt.Sprintf("%v", fields))

		// A healthy index has at least "text" indexed. If only "_id" is present
		// the index was built with a broken mapping (e.g. DefaultMapping.Enabled=false
		// in an earlier version) and every document is a bare ID with no searchable
		// content. Reset the state file so the next Index() call rebuilds from scratch.
		hasContent := false
		for _, f := range fields {
			if f == "text" || f == "name" || f == "description" {
				hasContent = true
				break
			}
		}
		if !hasContent {
			level.Warn(si.logger).Log("event", "index-stale-mapping",
				"msg", "search index has no searchable fields — it was built with an incompatible mapping",
				"fix", fmt.Sprintf("delete %s and %s, then restart", idxPath, statePath),
			)
		}
	}

	return si, nil
}

func buildSearchMapping() mapping.IndexMapping {
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Store = false
	textFieldMapping.IncludeInAll = true

	keywordFieldMapping := bleve.NewKeywordFieldMapping()
	keywordFieldMapping.Store = false
	keywordFieldMapping.IncludeInAll = false

	postMapping := bleve.NewDocumentMapping()
	postMapping.AddFieldMappingsAt("text", textFieldMapping)
	postMapping.AddFieldMappingsAt("channel", keywordFieldMapping)
	postMapping.AddFieldMappingsAt("type", keywordFieldMapping)

	aboutMapping := bleve.NewDocumentMapping()
	aboutMapping.AddFieldMappingsAt("name", textFieldMapping)
	aboutMapping.AddFieldMappingsAt("description", textFieldMapping)
	aboutMapping.AddFieldMappingsAt("type", keywordFieldMapping)

	idxMapping := bleve.NewIndexMapping()
	idxMapping.AddDocumentMapping("post", postMapping)
	idxMapping.AddDocumentMapping("about", aboutMapping)
	idxMapping.TypeField = "type"
	// Keep DefaultMapping enabled so the _all composite field works for
	// type-routed documents. Unrecognized types (contact, vote, etc.) are
	// filtered out by extractSearchDoc before reaching bleve.

	return idxMapping
}

// searchDocument is the structure indexed into Bleve.
type searchDocument struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Channel     string `json:"channel,omitempty"`
}

// Index processes all unprocessed entries from the given log.
func (idx *SearchIndex) Index(rxlog margaret.Log[*multimsg.MultiMessage]) error {
	lastSeq := idx.LastProcessedSeq()
	level.Debug(idx.logger).Log("event", "index-run-start", "last_processed_seq", lastSeq, "log_seq", rxlog.Seq())

	var opts []margaret.QueryOption
	if lastSeq >= 0 {
		opts = append(opts, margaret.Gt(lastSeq))
	}

	qry := rxlog.Query(opts...)
	batch := idx.index.NewBatch()
	batchCount := 0
	lastBatchSeq := margaret.SeqEmpty
	totalProcessed := 0

	for seq, mm := range qry.Iter() {
		if mm.Message != nil {
			if doc, docType := extractSearchDoc(mm); doc != nil {
				docID := strconv.FormatInt(seq, 10)
				doc.Type = docType
				batch.Index(docID, doc)
				batchCount++
			}
		}
		lastBatchSeq = seq
		totalProcessed++
		if idx.onEntry != nil {
			idx.onEntry()
		}

		if batchCount >= searchBatchSize {
			if err := idx.index.Batch(batch); err != nil {
				return fmt.Errorf("search index: batch error: %w", err)
			}
			idx.saveSeq(lastBatchSeq)
			batch = idx.index.NewBatch()
			batchCount = 0
		}
	}

	// flush remainder
	if batchCount > 0 {
		if err := idx.index.Batch(batch); err != nil {
			return fmt.Errorf("search index: batch error: %w", err)
		}
	}
	if totalProcessed > 0 {
		idx.saveSeq(lastBatchSeq)
	}

	level.Debug(idx.logger).Log("event", "index-run-done", "total_scanned", totalProcessed, "last_seq", lastBatchSeq)
	return qry.Err()
}

func extractSearchDoc(mm *multimsg.MultiMessage) (*searchDocument, string) {
	msg := mm.Message
	content := msg.ContentBytes()
	if len(content) == 0 || content[0] != '{' {
		return nil, "" // encrypted or empty
	}

	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(content, &header); err != nil {
		return nil, ""
	}

	switch header.Type {
	case "post":
		var post struct {
			Text    string `json:"text"`
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(content, &post); err != nil || post.Text == "" {
			return nil, ""
		}
		return &searchDocument{
			Text:    post.Text,
			Channel: post.Channel,
		}, "post"

	case "about":
		var about struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		}
		if err := json.Unmarshal(content, &about); err != nil {
			return nil, ""
		}
		if about.Name == "" && about.Description == "" {
			return nil, ""
		}
		return &searchDocument{
			Name:        about.Name,
			Description: about.Description,
		}, "about"

	default:
		return nil, ""
	}
}

func (idx *SearchIndex) saveSeq(seq int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	persist.Save(idx.stateFile, seq)
}

// LastProcessedSeq returns the sequence number of the last processed message.
func (idx *SearchIndex) LastProcessedSeq() int64 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var seq int64
	if err := persist.Load(idx.stateFile, &seq); err != nil {
		return margaret.SeqEmpty
	}
	return seq
}

// Search executes a full-text query and returns a bitmap of matching receive log sequence numbers.
func (idx *SearchIndex) Search(_ context.Context, queryStr string, limit int) (*sroar.Bitmap, error) {
	if limit <= 0 {
		limit = 100
	}

	docCount, _ := idx.index.DocCount()
	level.Debug(idx.logger).Log("event", "search-query", "query", queryStr, "limit", limit, "bleve_doc_count", docCount, "last_processed_seq", idx.LastProcessedSeq())

	// Search text, name and description fields explicitly.
	// NewQueryStringQuery searches the _all composite field, which is not
	// populated for type-routed documents (post/about use their own
	// DocumentMapping rather than the DefaultMapping where _all lives).
	textQ := bleve.NewMatchQuery(queryStr)
	textQ.SetField("text")
	nameQ := bleve.NewMatchQuery(queryStr)
	nameQ.SetField("name")
	descQ := bleve.NewMatchQuery(queryStr)
	descQ.SetField("description")
	q := bleve.NewDisjunctionQuery(textQ, nameQ, descQ)
	req := bleve.NewSearchRequestOptions(q, limit, 0, false)

	result, err := idx.index.Search(req)
	if err != nil {
		level.Debug(idx.logger).Log("event", "search-query-error", "query", queryStr, "err", err)
		return nil, fmt.Errorf("search index: query error: %w", err)
	}

	level.Debug(idx.logger).Log("event", "search-bleve-result", "query", queryStr, "total_hits", result.Total, "returned_hits", len(result.Hits))

	bm := sroar.NewBitmap()
	for _, hit := range result.Hits {
		seq, err := strconv.ParseInt(hit.ID, 10, 64)
		if err != nil {
			continue
		}
		bm.Set(uint64(seq))
	}

	level.Debug(idx.logger).Log("event", "search-bitmap-result", "query", queryStr, "cardinality", bm.GetCardinality())
	return bm, nil
}

// Close closes the Bleve index and state file.
func (idx *SearchIndex) Close() error {
	err := idx.index.Close()
	if err2 := idx.stateFile.Close(); err == nil {
		err = err2
	}
	return err
}
