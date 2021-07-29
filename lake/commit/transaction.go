package commit

import (
	"context"
	"errors"
	"io"

	"github.com/brimdata/zed/lake/commit/actions"
	"github.com/brimdata/zed/lake/index"
	"github.com/brimdata/zed/lake/journal"
	"github.com/brimdata/zed/lake/segment"
	"github.com/brimdata/zed/pkg/nano"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/segmentio/ksuid"
)

type Transaction struct {
	ID      ksuid.KSUID         `zng:"id"`
	Actions []actions.Interface `zng:"actions"`
}

func newTransaction(id ksuid.KSUID, capacity int) *Transaction {
	return &Transaction{
		ID:      id,
		Actions: make([]actions.Interface, 0, capacity),
	}
}

func NewCommitTxn(id ksuid.KSUID, date nano.Ts, author, message string, segments []segment.Reference) *Transaction {
	txn := newTransaction(id, len(segments)+1)
	if date == 0 {
		date = nano.Now()
	}
	txn.appendAdds(segments)
	txn.AppendCommitMessage(id, date, author, message)
	return txn
}

func NewAddsTxn(id ksuid.KSUID, segments []segment.Reference) *Transaction {
	txn := newTransaction(id, len(segments))
	txn.appendAdds(segments)
	return txn
}

func NewDeletesTxn(id ksuid.KSUID, ids []ksuid.KSUID) *Transaction {
	txn := newTransaction(id, len(ids))
	for _, id := range ids {
		txn.appendDelete(id)
	}
	return txn
}

func NewAddIndicesTxn(id ksuid.KSUID, indices []*index.Reference) *Transaction {
	txn := newTransaction(id, len(indices))
	for _, index := range indices {
		txn.appendAddIndex(index)
	}
	return txn
}

func (t *Transaction) Append(action actions.Interface) {
	t.Actions = append(t.Actions, action)
}

func (t *Transaction) AppendCommitMessage(id ksuid.KSUID, date nano.Ts, author, message string) {
	t.Append(&actions.CommitMessage{
		Commit:  id,
		Date:    date,
		Author:  author,
		Message: message,
	})
}

func (t *Transaction) appendAdds(segments []segment.Reference) {
	for _, s := range segments {
		t.Append(&actions.Add{Commit: t.ID, Segment: s})
	}
}

func (t *Transaction) appendAdd(s *segment.Reference) {
	t.Append(&actions.Add{Commit: t.ID, Segment: *s})
}

func (t *Transaction) appendDelete(id ksuid.KSUID) {
	t.Append(&actions.Delete{Commit: t.ID, ID: id})
}

func (t *Transaction) appendAddIndex(i *index.Reference) {
	t.Append(&actions.AddIndex{Commit: t.ID, Index: *i})
}

func (t Transaction) Serialize() ([]byte, error) {
	writer := journal.NewSerializer()
	for _, action := range t.Actions {
		if err := writer.Write(action); err != nil {
			writer.Close()
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	b := writer.Bytes()
	if len(b) == 0 {
		return nil, errors.New("empty transaction")
	}
	return b, nil
}

func (t *Transaction) Deserialize(r io.Reader) error {
	reader := journal.NewDeserializer(r, actions.JournalTypes)
	for {
		entry, err := reader.Read()
		if err != nil {
			return err
		}
		if entry == nil {
			break
		}
		action, ok := entry.(actions.Interface)
		if !ok {
			return badEntry(entry)
		}
		t.Append(action)
	}
	return nil
}

func LoadTransaction(ctx context.Context, engine storage.Engine, id ksuid.KSUID, uri *storage.URI) (*Transaction, error) {
	r, err := engine.Get(ctx, uri)
	if err != nil {
		return nil, err
	}
	t := newTransaction(id, 0)
	err = t.Deserialize(r)
	if closeErr := r.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}
