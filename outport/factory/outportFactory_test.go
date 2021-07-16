package factory

import (
	"errors"
	"testing"

	indexerFactory "github.com/ElrondNetwork/elastic-indexer-go/factory"
	"github.com/ElrondNetwork/elrond-go/outport"
	notifierFactory "github.com/ElrondNetwork/notifier-go/factory"
	"github.com/stretchr/testify/require"
)

func createMockArgsOutportHandler() *OutportFactoryArgs {
	mockElasticArgs := &indexerFactory.ArgsIndexerFactory{}
	mockNotifierArgs := &notifierFactory.EventNotifierFactoryArgs{}
	return &OutportFactoryArgs{
		ElasticIndexerFactoryArgs: mockElasticArgs,
		EventNotifierFactoryArgs:  mockNotifierArgs,
	}
}

func TestNewIndexerFactory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		argsFunc func() *OutportFactoryArgs
		exError  error
	}{
		{
			name: "NilArgsOutportFactory",
			argsFunc: func() *OutportFactoryArgs {
				return nil
			},
			exError: outport.ErrNilArgsOutportFactory,
		},
		{
			name: "AllOkShouldWork",
			argsFunc: func() *OutportFactoryArgs {
				return createMockArgsOutportHandler()
			},
			exError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CreateOutport(tt.argsFunc())
			require.True(t, errors.Is(err, tt.exError))
		})
	}
}
