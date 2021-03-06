package proof

import (
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/ElrondNetwork/elrond-go/api/errors"
	"github.com/ElrondNetwork/elrond-go/api/middleware"
	"github.com/ElrondNetwork/elrond-go/api/shared"
	"github.com/ElrondNetwork/elrond-go/api/wrapper"
	"github.com/gin-gonic/gin"
)

const (
	getProofCurrentRootHashEndpoint = "/proof/address/:address"
	getProofEndpoint                = "/proof/root-hash/:roothash/address/:address"
	verifyProofEndpoint             = "/proof/verify"

	getProofCurrentRootHashPath = "/address/:address"
	getProofPath                = "/root-hash/:roothash/address/:address"
	verifyProofPath             = "/verify"
)

// FacadeHandler interface defines methods that can be used by the gin webserver
type FacadeHandler interface {
	GetProof(rootHash string, address string) ([][]byte, error)
	GetProofCurrentRootHash(address string) ([][]byte, []byte, error)
	VerifyProof(rootHash string, address string, proof [][]byte) (bool, error)
}

// Routes defines Merkle proof related routes
func Routes(router *wrapper.RouterWrapper) {
	router.RegisterHandler(
		http.MethodGet,
		getProofPath,
		middleware.CreateEndpointThrottler(getProofEndpoint),
		GetProof,
	)
	router.RegisterHandler(
		http.MethodGet,
		getProofCurrentRootHashPath,
		middleware.CreateEndpointThrottler(getProofCurrentRootHashEndpoint),
		GetProofCurrentRootHash,
	)
	router.RegisterHandler(
		http.MethodPost,
		verifyProofPath,
		middleware.CreateEndpointThrottler(verifyProofEndpoint),
		VerifyProof,
	)
}

// VerifyProofRequest represents the parameters needed to verify a Merkle proof
type VerifyProofRequest struct {
	RootHash string   `json:"roothash"`
	Address  string   `json:"address"`
	Proof    []string `json:"proof"`
}

// GetProof will receive a rootHash and an address from the client, and it will return the Merkle proof
func GetProof(c *gin.Context) {
	facade, ok := getFacade(c)
	if !ok {
		return
	}

	rootHash := c.Param("roothash")
	if rootHash == "" {
		c.JSON(
			http.StatusBadRequest,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: fmt.Sprintf("%s: %s", errors.ErrValidation.Error(), errors.ErrValidationEmptyRootHash.Error()),
				Code:  shared.ReturnCodeRequestError,
			},
		)
		return
	}

	address := c.Param("address")
	if address == "" {
		c.JSON(
			http.StatusBadRequest,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: fmt.Sprintf("%s: %s", errors.ErrValidation.Error(), errors.ErrValidationEmptyAddress.Error()),
				Code:  shared.ReturnCodeRequestError,
			},
		)
		return
	}

	proof, err := facade.GetProof(rootHash, address)
	if err != nil {
		c.JSON(
			http.StatusInternalServerError,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: fmt.Sprintf("%s: %s", errors.ErrGetProof.Error(), err.Error()),
				Code:  shared.ReturnCodeInternalError,
			},
		)
		return
	}

	hexProof := make([]string, 0)
	for _, byteProof := range proof {
		hexProof = append(hexProof, hex.EncodeToString(byteProof))
	}

	c.JSON(
		http.StatusOK,
		shared.GenericAPIResponse{
			Data:  gin.H{"proof": hexProof},
			Error: "",
			Code:  shared.ReturnCodeSuccess,
		},
	)
}

// GetProofCurrentRootHash will receive an address from the client, and it will return the
// Merkle proof for the current root hash
func GetProofCurrentRootHash(c *gin.Context) {
	facade, ok := getFacade(c)
	if !ok {
		return
	}

	address := c.Param("address")
	if address == "" {
		c.JSON(
			http.StatusBadRequest,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: fmt.Sprintf("%s: %s", errors.ErrValidation.Error(), errors.ErrValidationEmptyAddress.Error()),
				Code:  shared.ReturnCodeRequestError,
			},
		)
		return
	}

	proof, rootHash, err := facade.GetProofCurrentRootHash(address)
	if err != nil {
		c.JSON(
			http.StatusInternalServerError,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: fmt.Sprintf("%s: %s", errors.ErrGetProof.Error(), err.Error()),
				Code:  shared.ReturnCodeInternalError,
			},
		)
		return
	}

	hexProof := make([]string, 0)
	for _, byteProof := range proof {
		hexProof = append(hexProof, hex.EncodeToString(byteProof))
	}

	c.JSON(
		http.StatusOK,
		shared.GenericAPIResponse{
			Data: gin.H{
				"proof":    hexProof,
				"rootHash": hex.EncodeToString(rootHash),
			},
			Error: "",
			Code:  shared.ReturnCodeSuccess,
		},
	)
}

// VerifyProof will receive a rootHash, an address and a Merkle proof from the client,
// and it will verify the proof
func VerifyProof(c *gin.Context) {
	facade, ok := getFacade(c)
	if !ok {
		return
	}

	var verifyProofParams = &VerifyProofRequest{}
	err := c.ShouldBindJSON(&verifyProofParams)
	if err != nil {
		c.JSON(
			http.StatusBadRequest,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: fmt.Sprintf("%s: %s", errors.ErrValidation.Error(), err.Error()),
				Code:  shared.ReturnCodeRequestError,
			},
		)
		return
	}

	proof := make([][]byte, 0)
	for _, hexProof := range verifyProofParams.Proof {
		bytesProof, err := hex.DecodeString(hexProof)
		if err != nil {
			c.JSON(
				http.StatusBadRequest,
				shared.GenericAPIResponse{
					Data:  nil,
					Error: fmt.Sprintf("%s: %s", errors.ErrValidation.Error(), err.Error()),
					Code:  shared.ReturnCodeRequestError,
				},
			)
			return
		}

		proof = append(proof, bytesProof)
	}

	ok, err = facade.VerifyProof(verifyProofParams.RootHash, verifyProofParams.Address, proof)
	if err != nil {
		c.JSON(
			http.StatusInternalServerError,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: fmt.Sprintf("%s: %s", errors.ErrVerifyProof.Error(), err.Error()),
				Code:  shared.ReturnCodeInternalError,
			},
		)
		return
	}

	c.JSON(
		http.StatusOK,
		shared.GenericAPIResponse{
			Data:  gin.H{"ok": ok},
			Error: "",
			Code:  shared.ReturnCodeSuccess,
		},
	)
}

func getFacade(c *gin.Context) (FacadeHandler, bool) {
	facadeObj, ok := c.Get("facade")
	if !ok {
		c.JSON(
			http.StatusInternalServerError,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: errors.ErrNilAppContext.Error(),
				Code:  shared.ReturnCodeInternalError,
			},
		)
		return nil, false
	}

	facade, ok := facadeObj.(FacadeHandler)
	if !ok {
		c.JSON(
			http.StatusInternalServerError,
			shared.GenericAPIResponse{
				Data:  nil,
				Error: errors.ErrInvalidAppContext.Error(),
				Code:  shared.ReturnCodeInternalError,
			},
		)
		return nil, false
	}

	return facade, true
}
