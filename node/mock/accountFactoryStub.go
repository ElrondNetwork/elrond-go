package mock

import "github.com/ElrondNetwork/elrond-go-sandbox/data/state"

type AccountsFactoryStub struct {
	CreateAccountCalled func(address state.AddressContainer, tracker state.AccountTracker) (state.AccountWrapper, error)
}

func (afs *AccountsFactoryStub) CreateAccount(address state.AddressContainer, tracker state.AccountTracker) (state.AccountWrapper, error) {
	return afs.CreateAccountCalled(address, tracker)
}
