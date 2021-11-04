package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	storageReader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := storageReader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{
        Value: value,
    }
	if value == nil {
        resp.NotFound = true
    }
	return resp, nil
}

func buildDeleteModify(cf string, key []byte) storage.Modify {
    return storage.Modify{
		Data: storage.Delete{
			Cf: cf,
            Key: key,
		},
    }
}

func buildPutModify(cf string, key []byte, value []byte) storage.Modify {
    return storage.Modify{
        Data: storage.Put{
            Cf: cf,
            Key: key,
            Value: value,
        },
    }
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	modify := buildPutModify(req.GetCf(), req.GetKey(), req.GetValue())
	resp := &kvrpcpb.RawPutResponse{}
	if err := server.storage.Write(nil, []storage.Modify{modify}); err != nil {
		return nil, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modify := buildDeleteModify(req.GetCf(), req.GetKey())
	resp := &kvrpcpb.RawDeleteResponse{}
	if err := server.storage.Write(nil, []storage.Modify{modify}); err != nil {
		return nil, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())

	var kvs []*kvrpcpb.KvPair
	cnt := req.Limit
	for cnt > 0 {
		item := iter.Item()
		if item == nil || !iter.Valid(){
            break
        }

		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
            Key: item.Key(),
            Value: value,
        })
		iter.Next()
		cnt--
	}

	resp := &kvrpcpb.RawScanResponse{
        Kvs: kvs,
    }

	return resp, nil
}
