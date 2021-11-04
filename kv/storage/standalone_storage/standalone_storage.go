package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	kvDb *badger.DB
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
    return &StandAloneStorageReader{txn: txn}
}

//	GetCF(cf string, key []byte) ([]byte, error)
//	IterCF(cf string) engine_util.DBIterator
//	Close()
func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	key = engine_util.KeyWithCF(cf, key)
	item, err := r.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
        }
		return nil, err
	}
	return item.Value()
}
func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}


func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	storage := &StandAloneStorage{}
	storage.kvDb = engine_util.CreateDB(conf.DBPath, conf.Raft)
	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.kvDb.NewTransaction(false)
	return NewStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.kvDb.Update(func(txn *badger.Txn) error {
        for _, m := range batch {
			switch m.Data.(type) {
			case storage.Delete:
				delete := m.Data.(storage.Delete)
				if err := txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key)); err != nil {
					return err
				}
			case storage.Put:
				put := m.Data.(storage.Put)
				if err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
					return err
				}
			}
		}
        return nil
    })
}
