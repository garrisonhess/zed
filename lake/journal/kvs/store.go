package kvs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/brimdata/zed/lake/journal"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/brimdata/zed/zson"
)

const (
	journalHandle = "C"
	maxRetries    = 10
)

var (
	ErrRetriesExceeded = fmt.Errorf("config journal unavailable after %d attempts", maxRetries)
	ErrKeyExists       = errors.New("key already exists")
	ErrNoSuchKey       = errors.New("no such key")
)

type Store struct {
	path        *storage.URI
	journal     *journal.Queue
	unmarshaler *zson.UnmarshalZNGContext
	table       map[string]interface{}
	at          journal.ID
	loadTime    time.Time
}

type Entry struct {
	Key   string
	Value interface{}
}

func newStore(path *storage.URI, valTypes []interface{}) *Store {
	u := zson.NewZNGUnmarshaler()
	u.Bind(valTypes...)
	u.Bind(Entry{})
	return &Store{
		path:        path,
		unmarshaler: u,
	}
}

func (s *Store) load(ctx context.Context) error {
	head, err := s.journal.ReadHead(ctx)
	if err != nil {
		return err
	}
	if head == s.at {
		return nil
	}
	r, err := s.journal.OpenAsZNG(ctx, head, 0)
	if err != nil {
		return err
	}
	table := make(map[string]interface{})
	for {
		rec, err := r.Read()
		if err != nil {
			return err
		}
		if rec == nil {
			s.table = table
			s.loadTime = time.Now()
			return nil

		}
		var e Entry
		if err := s.unmarshaler.Unmarshal(rec.Value, &e); err != nil {
			return err
		}
		// XXX we use nil value to remove items from table and there
		// is currently no way to store nil as a value.
		if e.Value == nil {
			delete(table, e.Key)
		} else {
			table[e.Key] = e.Value
		}
	}
}

func (s *Store) stale() bool {
	if s.loadTime.IsZero() {
		return true
	}
	return time.Now().Sub(s.loadTime) > time.Second
}

func (s *Store) Keys(ctx context.Context, key string) ([]string, error) {
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(s.table))
	for key := range s.table {
		keys = append(keys, key)
	}
	return keys, nil
}

func (s *Store) Values(ctx context.Context) ([]interface{}, error) {
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	vals := make([]interface{}, 0, len(s.table))
	for _, val := range s.table {
		vals = append(vals, val)
	}
	return vals, nil
}

func (s *Store) Lookup(ctx context.Context, key string) (interface{}, error) {
	var fresh bool
	if s.stale() {
		if err := s.load(ctx); err != nil {
			return nil, err
		}
		fresh = true
	}
	val, ok := s.table[key]
	if !ok {
		if fresh {
			return nil, ErrNoSuchKey
		}
		// If we didn't load the table, try loading it
		// then re-checking for the key.
		if err := s.load(ctx); err != nil {
			return nil, err
		}
		val, ok = s.table[key]
		if !ok {
			return nil, ErrNoSuchKey
		}
	}
	return val, nil
}

func (s *Store) Set(ctx context.Context, key string, value interface{}) error {
	e := Entry{
		Key:   key,
		Value: value,
	}
	b, err := e.serialize()
	if err != nil {
		return err
	}
	for attempts := 0; attempts < maxRetries; attempts++ {
		if err := s.load(ctx); err != nil {
			return err
		}
		if _, ok := s.table[key]; ok {
			return ErrKeyExists
		}
		err := s.journal.CommitAt(ctx, s.at+1, b)
		if err != nil {
			if os.IsExist(err) {
				time.Sleep(time.Millisecond)
				continue
			}
			return err
		}
		return nil
	}
	return ErrRetriesExceeded
}

func (e Entry) serialize() ([]byte, error) {
	serializer := journal.NewSerializer()
	if err := serializer.Write(&e); err != nil {
		return nil, err
	}
	return serializer.Bytes(), nil
}

func Open(ctx context.Context, engine storage.Engine, path *storage.URI, keyTypes []interface{}) (*Store, error) {
	s := newStore(path, keyTypes)
	var err error
	s.journal, err = journal.Open(ctx, engine, s.path.AppendPath(journalHandle))
	if err != nil {
		return nil, err
	}
	return s, nil
}

func Create(ctx context.Context, engine storage.Engine, path *storage.URI, keyTypes []interface{}) (*Store, error) {
	s := newStore(path, keyTypes)
	j, err := journal.Create(ctx, engine, s.path.AppendPath(journalHandle))
	if err != nil {
		return nil, err
	}
	s.journal = j
	return s, nil
}
