package mock

import (
	"bytes"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

// DatabaseWriterStub --
type DatabaseWriterStub struct {
	DoRequestCalled     func(req *esapi.IndexRequest) error
	DoBulkRequestCalled func(buff *bytes.Buffer, index string) error
	DoBulkRemoveCalled  func(index string, hashes []string) error
	DoMultiGetCalled    func(ids []string, index string, withSource bool, res interface{}) error
}

// DoRequest --
func (dws *DatabaseWriterStub) DoRequest(req *esapi.IndexRequest) error {
	if dws.DoRequestCalled != nil {
		return dws.DoRequestCalled(req)
	}
	return nil
}

// DoBulkRequest --
func (dws *DatabaseWriterStub) DoBulkRequest(buff *bytes.Buffer, index string) error {
	if dws.DoBulkRequestCalled != nil {
		return dws.DoBulkRequestCalled(buff, index)
	}
	return nil
}

// DoMultiGet --
func (dws *DatabaseWriterStub) DoMultiGet(ids []string, index string, withSource bool, res interface{}) error {
	if dws.DoMultiGetCalled != nil {
		return dws.DoMultiGetCalled(ids, index, withSource, res)
	}

	return nil
}

// DoBulkRemove -
func (dws *DatabaseWriterStub) DoBulkRemove(index string, hashes []string) error {
	if dws.DoBulkRemoveCalled != nil {
		return dws.DoBulkRemoveCalled(index, hashes)
	}

	return nil
}

// CheckAndCreateIndex --
func (dws *DatabaseWriterStub) CheckAndCreateIndex(_ string) error {
	return nil
}

// CheckAndCreateAlias -
func (dws *DatabaseWriterStub) CheckAndCreateAlias(_ string, _ string) error {
	return nil
}

// CheckAndCreateTemplate -
func (dws *DatabaseWriterStub) CheckAndCreateTemplate(_ string, _ *bytes.Buffer) error {
	return nil
}

// CheckAndCreatePolicy -
func (dws *DatabaseWriterStub) CheckAndCreatePolicy(_ string, _ *bytes.Buffer) error {
	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (dws *DatabaseWriterStub) IsInterfaceNil() bool {
	return dws == nil
}
