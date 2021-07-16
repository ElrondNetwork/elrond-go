package factory

import (
	indexerFactory "github.com/ElrondNetwork/elastic-indexer-go/factory"
	"github.com/ElrondNetwork/elrond-go/outport"
)

// CreateOutport will create a new instance of OutportHandler
func CreateOutport(args *indexerFactory.ArgsIndexerFactory) (outport.OutportHandler, error) {
	err := checkArguments(args)
	if err != nil {
		return nil, err
	}

	outportHandler := outport.NewOutport()
	err = createAndSubscribeDrivers(outportHandler, args)
	if err != nil {
		return nil, err
	}

	return outportHandler, nil
}

func createAndSubscribeDrivers(outport outport.OutportHandler, args *indexerFactory.ArgsIndexerFactory) error {
	err := createAndSubscribeElasticDriverIfNeeded(outport, args)
	if err != nil {
		return err
	}

	return nil
}

func createAndSubscribeElasticDriverIfNeeded(
	outport outport.OutportHandler,
	args *indexerFactory.ArgsIndexerFactory,
) error {
	if !args.Enabled {
		return nil
	}

	elasticDriver, err := indexerFactory.NewIndexer(args)
	if err != nil {
		return err
	}

	return outport.SubscribeDriver(elasticDriver)
}

func checkArguments(args *indexerFactory.ArgsIndexerFactory) error {
	if args == nil {
		return outport.ErrNilArgsElasticDriverFactory
	}

	return nil
}
