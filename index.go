package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/blevesearch/bleve"
	mp "github.com/blevesearch/bleve/mapping"
	"github.com/yanyiwu/gojieba"
	// _ "github.com/yanyiwu/gojieba/bleve"
)

// Indexer represents the indexing engine.
type Indexer struct {
	path    string // Path to bleve storage
	batchSz int    // Indexing batch size

	shards []bleve.Index    // Index shards i.e. bleve indexes
	alias  bleve.IndexAlias // All bleve indexes as one reference, for search
}

func (i *Indexer) IdexerHub() bleve.IndexAlias {
	return i.alias
}

// New returns a new indexer.
func New(path string, nShards, batchSz int) *Indexer {
	return &Indexer{
		path:    path,
		batchSz: batchSz,
		shards:  make([]bleve.Index, 0, nShards),
		alias:   bleve.NewIndexAlias(),
	}
}

// Open opens the indexer, preparing it for indexing.
func (i *Indexer) Open() error {
	if err := os.MkdirAll(i.path, 0755); err != nil {
		return fmt.Errorf("unable to create index directory %s", i.path)
	}

	for s := 0; s < cap(i.shards); s++ {
		path := filepath.Join(i.path, strconv.Itoa(s))
		b, err := bleve.Open(path)
		if err == bleve.ErrorIndexPathDoesNotExist {
			b, err := bleve.New(path, mapping())
			if err != nil {
				return err
			}
			i.shards = append(i.shards, b)
			i.alias.Add(b)
		} else if err != nil {
			return err
		} else {
			i.shards = append(i.shards, b)
			i.alias.Add(b)
		}
	}

	return nil
}

// Index indexes the given docs, dividing the docs evenly across the shards.
// Blocks until all documents have been indexed.
func (i *Indexer) Index(docs []string) error {
	base := 0
	docsPerShard := (len(docs) / len(i.shards))
	var wg sync.WaitGroup

	wg.Add(len(i.shards))
	for _, s := range i.shards {
		go func(b bleve.Index, ds []string) {
			defer wg.Done()

			batch := b.NewBatch()
			n := 0

			// Just index whole batches.
			for n = 0; n < len(ds)-(len(ds)%i.batchSz); n++ {
				data := struct {
					Body string
				}{
					Body: ds[n],
				}

				if err := batch.Index(strconv.Itoa(n), data); err != nil {
					panic(fmt.Sprintf("failed to index doc: %s", err.Error()))
				}

				if batch.Size() == i.batchSz {
					if err := b.Batch(batch); err != nil {
						panic(fmt.Sprintf("failed to index batch: %s", err.Error()))
					}
					batch = b.NewBatch()
				}
			}
		}(s, docs[base:base+docsPerShard])
		base = base + docsPerShard
	}

	wg.Wait()
	return nil
}

// Count returns the total number of documents indexed.
func (i *Indexer) Count() (uint64, error) {
	return i.alias.DocCount()
}

func mapping() mp.IndexMapping {
	// a generic reusable mapping for english text
	standardJustIndexed := bleve.NewTextFieldMapping()
	standardJustIndexed.Store = false
	standardJustIndexed.IncludeInAll = false
	standardJustIndexed.IncludeTermVectors = false
	standardJustIndexed.Analyzer = "gojieba"

	articleMapping := bleve.NewDocumentMapping()

	// body
	articleMapping.AddFieldMappingsAt("Body", standardJustIndexed)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = articleMapping
	indexMapping.DefaultAnalyzer = "gojieba"

	err := indexMapping.AddCustomTokenizer("gojieba",
		map[string]interface{}{
			"dictpath":     gojieba.DICT_PATH,
			"hmmpath":      gojieba.HMM_PATH,
			"userdictpath": gojieba.USER_DICT_PATH,
			"idf":          gojieba.IDF_PATH,
			"stop_words":   gojieba.STOP_WORDS_PATH,
			"type":         "gojieba",
		},
	)
	if err != nil {
		panic(err)
	}
	err = indexMapping.AddCustomAnalyzer("gojieba",
		map[string]interface{}{
			"type":      "gojieba",
			"tokenizer": "gojieba",
		},
	)
	if err != nil {
		panic(err)
	}

	return indexMapping
}
