package trie2_test

import (
	"strconv"
	"testing"

	"github.com/ElrondNetwork/elrond-go-sandbox/data/trie2"
	"github.com/ElrondNetwork/elrond-go-sandbox/hashing/keccak"
	"github.com/ElrondNetwork/elrond-go-sandbox/marshal"
	"github.com/ElrondNetwork/elrond-go-sandbox/storage/memorydb"
	"github.com/stretchr/testify/assert"
)

func testTrie2(nr int) (trie2.Trie, [][]byte) {
	db, _ := memorydb.New()
	tr, _ := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})

	var values [][]byte
	hsh := keccak.Keccak{}

	for i := 0; i < nr; i++ {
		values = append(values, hsh.Compute(string(i)))
		tr.Update(values[i], values[i])
	}

	return tr, values

}

func testTrie() trie2.Trie {
	db, _ := memorydb.New()
	tr, _ := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})

	tr.Update([]byte("doe"), []byte("reindeer"))
	tr.Update([]byte("dog"), []byte("puppy"))
	tr.Update([]byte("dogglesworth"), []byte("cat"))

	return tr
}

func TestNewTrieWithNilDB(t *testing.T) {
	tr, err := trie2.NewTrie(nil, marshal.JsonMarshalizer{}, keccak.Keccak{})

	assert.Nil(t, tr)
	assert.NotNil(t, err)
}

func TestNewTrieWithNilMarshalizer(t *testing.T) {
	db, _ := memorydb.New()
	tr, err := trie2.NewTrie(db, nil, keccak.Keccak{})

	assert.Nil(t, tr)
	assert.NotNil(t, err)
}

func TestNewTrieWithNilHasher(t *testing.T) {
	db, _ := memorydb.New()
	tr, err := trie2.NewTrie(db, marshal.JsonMarshalizer{}, nil)

	assert.Nil(t, tr)
	assert.NotNil(t, err)
}

func TestPatriciaMerkleTree_Get(t *testing.T) {
	tr, val := testTrie2(10000)

	for i := range val {
		v, _ := tr.Get(val[i])
		assert.Equal(t, val[i], v)
	}
}

func TestPatriciaMerkleTree_GetEmptyTrie(t *testing.T) {
	db, _ := memorydb.New()
	tr, _ := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})

	val, err := tr.Get([]byte("dog"))
	assert.Equal(t, trie2.ErrNilNode, err)
	assert.Nil(t, val)
}

func TestPatriciaMerkleTree_Update(t *testing.T) {
	tr := testTrie()

	newVal := []byte("doge")
	tr.Update([]byte("dog"), newVal)

	val, _ := tr.Get([]byte("dog"))
	assert.Equal(t, newVal, val)
}

func TestPatriciaMerkleTree_UpdateEmptyVal(t *testing.T) {
	tr := testTrie()
	var empty []byte

	tr.Update([]byte("doe"), []byte{})

	v, _ := tr.Get([]byte("doe"))
	assert.Equal(t, empty, v)
}

func TestPatriciaMerkleTree_UpdateNotExisting(t *testing.T) {
	tr := testTrie()

	tr.Update([]byte("does"), []byte("this"))

	v, _ := tr.Get([]byte("does"))
	assert.Equal(t, []byte("this"), v)
}

func TestPatriciaMerkleTree_Delete(t *testing.T) {
	tr := testTrie()
	var empty []byte

	tr.Delete([]byte("doe"))

	v, _ := tr.Get([]byte("doe"))
	assert.Equal(t, empty, v)
}

func TestPatriciaMerkleTree_DeleteEmptyTrie(t *testing.T) {
	db, _ := memorydb.New()
	tr, _ := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})

	err := tr.Delete([]byte("dog"))
	assert.Nil(t, err)
}

func TestPatriciaMerkleTree_Root(t *testing.T) {
	tr := testTrie()

	root, err := tr.Root()
	assert.NotNil(t, root)
	assert.Nil(t, err)
}

func TestPatriciaMerkleTree_NilRoot(t *testing.T) {
	db, _ := memorydb.New()
	tr, _ := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})

	root, err := tr.Root()
	assert.Equal(t, trie2.ErrNilNode, err)
	assert.Nil(t, root)
}

func TestPatriciaMerkleTree_Prove(t *testing.T) {
	tr := testTrie()

	proof, err := tr.Prove([]byte("dog"))
	assert.Nil(t, err)
	ok, _ := tr.VerifyProof(proof, []byte("dog"))
	assert.True(t, ok)
}

func TestPatriciaMerkleTree_ProveCollapsedTrie(t *testing.T) {
	tr := testTrie()
	tr.Commit()

	proof, err := tr.Prove([]byte("dog"))
	assert.Nil(t, err)
	ok, _ := tr.VerifyProof(proof, []byte("dog"))
	assert.True(t, ok)
}

func TestPatriciaMerkleTree_ProveOnEmptyTrie(t *testing.T) {
	db, _ := memorydb.New()
	tr, _ := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})

	proof, err := tr.Prove([]byte("dog"))
	assert.Nil(t, proof)
	assert.Equal(t, trie2.ErrNilNode, err)
}

func TestPatriciaMerkleTree_VerifyProof(t *testing.T) {
	tr, val := testTrie2(50)

	for i := range val {
		proof, _ := tr.Prove(val[i])

		ok, err := tr.VerifyProof(proof, val[i])
		assert.Nil(t, err)
		assert.True(t, ok)

		ok, err = tr.VerifyProof(proof, []byte("dog"+strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.False(t, ok)
	}

}

func TestPatriciaMerkleTree_VerifyProofNilProofs(t *testing.T) {
	tr := testTrie()

	ok, err := tr.VerifyProof(nil, []byte("dog"))
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestPatriciaMerkleTree_VerifyProofEmptyProofs(t *testing.T) {
	tr := testTrie()

	ok, err := tr.VerifyProof([][]byte{}, []byte("dog"))
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestPatriciaMerkleTree_Consistency(t *testing.T) {
	tr := testTrie()
	root1, _ := tr.Root()

	tr.Update([]byte("dodge"), []byte("viper"))
	root2, _ := tr.Root()

	tr.Delete([]byte("dodge"))
	root3, _ := tr.Root()

	assert.Equal(t, root1, root3)
	assert.NotEqual(t, root1, root2)
}

func TestPatriciaMerkleTree_Commit(t *testing.T) {
	tr := testTrie()

	err := tr.Commit()
	assert.Nil(t, err)
}

func TestPatriciaMerkleTree_CommitAfterCommit(t *testing.T) {
	tr := testTrie()

	tr.Commit()
	err := tr.Commit()
	assert.Nil(t, err)
}

func TestPatriciaMerkleTree_CommitEmptyRoot(t *testing.T) {
	db, _ := memorydb.New()
	tr, _ := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})

	err := tr.Commit()
	assert.Equal(t, trie2.ErrNilNode, err)
}

func TestPatriciaMerkleTree_GetAfterCommit(t *testing.T) {
	tr := testTrie()

	err := tr.Commit()
	assert.Nil(t, err)

	val, err := tr.Get([]byte("dog"))
	assert.Equal(t, []byte("puppy"), val)
	assert.Nil(t, err)
}

func TestPatriciaMerkleTree_InsertAfterCommit(t *testing.T) {
	tr1 := testTrie()
	tr2 := testTrie()

	err := tr1.Commit()
	assert.Nil(t, err)

	tr1.Update([]byte("doge"), []byte("coin"))
	tr2.Update([]byte("doge"), []byte("coin"))

	root1, _ := tr1.Root()
	root2, _ := tr2.Root()

	assert.Equal(t, root2, root1)

}

func TestPatriciaMerkleTree_DeleteAfterCommit(t *testing.T) {
	tr1 := testTrie()
	tr2 := testTrie()

	err := tr1.Commit()
	assert.Nil(t, err)

	tr1.Delete([]byte("dogglesworth"))
	tr2.Delete([]byte("dogglesworth"))

	root1, _ := tr1.Root()
	root2, _ := tr2.Root()

	assert.Equal(t, root2, root1)
}

func emptyTrie(b *testing.B) trie2.Trie {
	db, err := memorydb.New()
	assert.Nil(b, err)
	tr, err := trie2.NewTrie(db, marshal.JsonMarshalizer{}, keccak.Keccak{})
	assert.Nil(b, err)
	return tr
}

func BenchmarkPatriciaMerkleTree_Insert(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 1000000; i++ {
		val := hsh.Compute(strconv.Itoa(i))
		tr.Update(val, val)
	}
	for i := 1000000; i < 10000000; i++ {
		values = append(values, hsh.Compute(strconv.Itoa(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := tr.Update(values[i%9000000], values[i%9000000])
		assert.Nil(b, err)
	}
}

func BenchmarkPatriciaMerkleTree_InsertCollapsedTrie(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 1000000; i++ {
		val := hsh.Compute(strconv.Itoa(i))
		tr.Update(val, val)
	}
	for i := 1000000; i < 10000000; i++ {
		values = append(values, hsh.Compute(strconv.Itoa(i)))
	}
	err := tr.Commit()
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := tr.Update(values[i%9000000], values[i%9000000])
		assert.Nil(b, err)
	}
}

func BenchmarkPatriciaMerkleTree_Delete(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 3000000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := tr.Delete(values[i%3000000])
		assert.Nil(b, err)
	}
}

func BenchmarkPatriciaMerkleTree_DeleteCollapsedTrie(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 3000000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}
	err := tr.Commit()
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := tr.Delete(values[i%3000000])
		assert.Nil(b, err)
	}
}

func BenchmarkPatriciaMerkleTree_Get(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 3000000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val, err := tr.Get(values[i%3000000])
		assert.Nil(b, err)
		assert.Equal(b, values[i%3000000], val)
	}
}

func BenchmarkPatriciaMerkleTree_GetCollapsedTrie(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 3000000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}
	err := tr.Commit()
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val, err := tr.Get(values[i%3000000])
		assert.Nil(b, err)
		assert.Equal(b, values[i%3000000], val)
	}
}

func BenchmarkPatriciaMerkleTree_Prove(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 3000000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proof, err := tr.Prove(values[i%3000000])
		assert.Nil(b, err)
		assert.NotNil(b, proof)
	}
}

func BenchmarkPatriciaMerkleTree_ProveCollapsedTrie(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte

	for i := 0; i < 3000000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}
	err := tr.Commit()
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proof, err := tr.Prove(values[i%3000000])
		assert.Nil(b, err)
		assert.NotNil(b, proof)
	}
}

func BenchmarkPatriciaMerkleTree_VerifyProof(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte
	var proofs [][][]byte

	for i := 0; i < 100000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}
	for i := 0; i < 10; i++ {
		proof, err := tr.Prove(values[i])
		assert.Nil(b, err)
		proofs = append(proofs, proof)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := tr.VerifyProof(proofs[i%10], values[i%10])
		assert.True(b, ok)
		assert.Nil(b, err)
	}
}

func BenchmarkPatriciaMerkleTree_VerifyProofCollapsedTrie(b *testing.B) {
	tr := emptyTrie(b)
	hsh := keccak.Keccak{}
	var values [][]byte
	var proofs [][][]byte

	for i := 0; i < 100000; i++ {
		hash := hsh.Compute(strconv.Itoa(i))
		values = append(values, hash)
		tr.Update(hash, hash)
	}
	for i := 0; i < 10; i++ {
		proof, err := tr.Prove(values[i])
		assert.Nil(b, err)
		proofs = append(proofs, proof)
	}
	err := tr.Commit()
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := tr.VerifyProof(proofs[i%10], values[i%10])
		assert.True(b, ok)
		assert.Nil(b, err)
	}
}

func BenchmarkPatriciaMerkleTree_Commit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		hsh := keccak.Keccak{}
		tr := emptyTrie(b)
		for i := 0; i < 1000000; i++ {
			hash := hsh.Compute(strconv.Itoa(i))
			err := tr.Update(hash, hash)
			assert.Nil(b, err)
		}
		b.StartTimer()

		err := tr.Commit()
		assert.Nil(b, err)
	}
}
