package pathmanager

import (
	"fmt"
	"strings"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/storage"
)

var _ storage.PathManagerHandler = (*PathManager)(nil)

// PathManager will handle creation of paths for storers
type PathManager struct {
	databasePath        string
	pruningPathTemplate string
	staticPathTemplate  string
}

// NewPathManager will return a new instance of PathManager if the provided arguments are fine
func NewPathManager(pruningPathTemplate string, staticPathTemplate string, databasePath string) (*PathManager, error) {
	if len(pruningPathTemplate) == 0 {
		return nil, storage.ErrEmptyPruningPathTemplate
	}
	if !strings.Contains(pruningPathTemplate, core.PathEpochPlaceholder) ||
		!strings.Contains(pruningPathTemplate, core.PathShardPlaceholder) ||
		!strings.Contains(pruningPathTemplate, core.PathIdentifierPlaceholder) {
		return nil, storage.ErrInvalidPruningPathTemplate
	}

	if len(staticPathTemplate) == 0 {
		return nil, storage.ErrEmptyStaticPathTemplate
	}
	if !strings.Contains(staticPathTemplate, core.PathShardPlaceholder) ||
		!strings.Contains(staticPathTemplate, core.PathIdentifierPlaceholder) {
		return nil, storage.ErrInvalidStaticPathTemplate
	}

	if len(databasePath) == 0 {
		return nil, storage.ErrInvalidDatabasePath
	}

	return &PathManager{
		pruningPathTemplate: pruningPathTemplate,
		staticPathTemplate:  staticPathTemplate,
		databasePath:        databasePath,
	}, nil
}

// PathForEpoch will return the new path for a pruning storer
func (pm *PathManager) PathForEpoch(shardId string, epoch uint32, identifier string) string {
	path := pm.pruningPathTemplate
	path = strings.Replace(path, core.PathEpochPlaceholder, fmt.Sprintf("%d", epoch), 1)
	path = strings.Replace(path, core.PathShardPlaceholder, shardId, 1)
	path = strings.Replace(path, core.PathIdentifierPlaceholder, identifier, 1)

	return path
}

// PathForStatic will return the path for a static storer
func (pm *PathManager) PathForStatic(shardId string, identifier string) string {
	path := pm.staticPathTemplate
	path = strings.Replace(path, core.PathShardPlaceholder, shardId, 1)
	path = strings.Replace(path, core.PathIdentifierPlaceholder, identifier, 1)

	return path
}

// DatabasePath returns the path for the databases directory
func (pm *PathManager) DatabasePath() string {
	return pm.databasePath
}

// IsInterfaceNil returns true if there is no value under the interface
func (pm *PathManager) IsInterfaceNil() bool {
	return pm == nil
}
