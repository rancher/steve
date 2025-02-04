package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	m, err := NewManager()
	if err != nil {
		t.FailNow()
	}
	assert.NotNil(t, m)
}

func TestEncrypt(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase

	tests = append(tests, testCase{description: "test encrypt with arbitrary initial key", test: func(t *testing.T) {
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181, 83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m, err := NewManager()
		require.Nil(t, err)

		m.dataKeys[0] = testDEK

		testData := []byte("something")
		cipherText, nonce, keyID, err := m.Encrypt(testData)
		require.Nil(t, err)

		dek := m.dataKeys[keyID]
		b, err := aes.NewCipher(dek)
		require.Nil(t, err)

		aead, err := cipher.NewGCM(b)
		require.Nil(t, err)
		decryptedData, err := aead.Open(nil, nonce, cipherText, nil)

		require.Nil(t, err)
		assert.Equal(t, testData, decryptedData)
	}})
	tests = append(tests, testCase{description: "test encrypt without arbitrary initial key", test: func(t *testing.T) {
		m, err := NewManager()
		require.Nil(t, err)

		testData := []byte("something")
		cipherText, nonce, keyID, err := m.Encrypt(testData)
		require.Nil(t, err)

		dek := m.dataKeys[keyID]
		b, err := aes.NewCipher(dek)
		require.Nil(t, err)

		aead, err := cipher.NewGCM(b)
		require.Nil(t, err)
		decryptedData, err := aead.Open(nil, nonce, cipherText, nil)

		require.Nil(t, err)
		assert.Equal(t, testData, decryptedData)
	}})
	tests = append(tests, testCase{description: "test encrypt: same data yield different cipher/nonce pair", test: func(t *testing.T) {
		m, err := NewManager()
		require.Nil(t, err)

		testData := []byte("something")
		cipher1, nonce1, keyID1, err := m.Encrypt(testData)
		require.Nil(t, err)
		assert.Len(t, cipher1, 25)
		assert.Len(t, nonce1, 12)
		assert.NotEmpty(t, cipher1)
		assert.NotEmpty(t, nonce1)

		cipher2, nonce2, keyID2, err := m.Encrypt(testData)
		require.Nil(t, err)

		assert.Equal(t, keyID1, keyID2)
		assert.NotEqual(t, cipher1, cipher2, "each encrypt op must return a unique cipher")
		assert.NotEqual(t, nonce1, nonce2, "each encrypt op must return a unique nonce")
	}})
	tests = append(tests, testCase{description: "test encrypt with key rotation", test: func(t *testing.T) {
		m, err := NewManager()
		require.Nil(t, err)

		testData := []byte("something")
		cipher1, nonce1, keyID1, err := m.Encrypt(testData)
		require.Nil(t, err)
		assert.Len(t, cipher1, 25)
		assert.Len(t, nonce1, 12)
		assert.NotEmpty(t, cipher1)
		assert.NotEmpty(t, nonce1)

		m.activeKeyCounter += maxWriteCount

		cipher2, nonce2, keyID2, err := m.Encrypt(testData)
		require.Nil(t, err)

		assert.Equal(t, int64(1), m.activeKeyCounter)
		assert.NotEqual(t, keyID1, keyID2)
		assert.NotEqual(t, cipher1, cipher2, "each encrypt op must return a unique cipher")
		assert.NotEqual(t, nonce1, nonce2, "each encrypt op must return a unique nonce")
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestDecrypt(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	var tests []testCase

	tests = append(tests, testCase{description: "test decrypt with arbitrary key", test: func(t *testing.T) {
		testDEK := []byte{83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181, 83, 125, 203, 18, 75, 156, 24, 192, 119, 73, 157, 222, 143, 140, 231, 181}

		m, err := NewManager()
		require.Nil(t, err)

		m.dataKeys[0] = testDEK

		testData := []byte("something")

		// encrypt data out of band.
		b, err := aes.NewCipher(testDEK)
		require.Nil(t, err)

		aead, err := cipher.NewGCM(b)
		require.Nil(t, err)

		nonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(nonce)
		require.Nil(t, err)

		cipherText := aead.Seal(nil, nonce, testData, nil)

		// use manager to decrypt the data.
		decryptedData, err := m.Decrypt(cipherText, nonce, 0)
		require.Nil(t, err)

		assert.Equal(t, testData, decryptedData)
	},
	})
	tests = append(tests, testCase{description: "test decrypt without arbitrary key", test: func(t *testing.T) {
		m, err := NewManager()
		require.Nil(t, err)

		testData := []byte("something")

		// encrypt data out of band.
		dek := m.dataKeys[0]
		b, err := aes.NewCipher(dek)
		require.Nil(t, err)

		aead, err := cipher.NewGCM(b)
		require.Nil(t, err)

		nonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(nonce)
		require.Nil(t, err)

		cipherText := aead.Seal(nil, nonce, testData, nil)

		// use manager to decrypt the data.
		decryptedData, err := m.Decrypt(cipherText, nonce, 0)
		require.Nil(t, err)

		assert.Equal(t, testData, decryptedData)
	},
	})
	tests = append(tests, testCase{description: "test decrypt with wrong data nonce should return error", test: func(t *testing.T) {
		m, err := NewManager()
		require.Nil(t, err)

		testData := []byte("something")

		// encrypt data out of band.
		dek := m.dataKeys[0]
		b, err := aes.NewCipher(dek)
		require.Nil(t, err)

		aead, err := cipher.NewGCM(b)
		require.Nil(t, err)

		nonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(nonce)
		require.Nil(t, err)

		cipherText := aead.Seal(nil, nonce, testData, nil)

		// generate random nonce.
		randomNonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(nonce)
		require.Nil(t, err)

		// decrypted encrypted data using encrypted dek
		_, err = m.Decrypt(cipherText, randomNonce, 0)
		assert.NotNil(t, err)
	},
	})

	tests = append(tests, testCase{description: "test decrypt with DEK/nonce pair not used to encrypt should return error", test: func(t *testing.T) {
		m, err := NewManager()
		require.Nil(t, err)

		testData := []byte("something")

		// encrypt data out of band.
		dek := m.dataKeys[0]
		b, err := aes.NewCipher(dek)
		require.Nil(t, err)

		aead, err := cipher.NewGCM(b)
		require.Nil(t, err)

		nonce := make([]byte, aead.NonceSize())
		_, err = rand.Read(nonce)
		require.Nil(t, err)

		cipherText := aead.Seal(nil, nonce, testData, nil)

		key, id, err := m.newDataEncryptionKey()
		require.Nil(t, err)
		m.dataKeys[id] = key

		plainText, err := m.Decrypt(cipherText, nonce, id)
		assert.NotNil(t, err)
		assert.Nil(t, plainText)
	},
	})
	tests = append(tests, testCase{description: "test decrypt for non active key", test: func(t *testing.T) {
		m, err := NewManager()
		require.Nil(t, err)

		testData := []byte("something")

		cipher, nonce, keyID, err := m.Encrypt(testData)
		require.Nil(t, err)

		// force key rotation.
		m.activeKeyCounter += maxWriteCount
		_, _, newKeyID, err := m.Encrypt(nil)
		require.Nil(t, err)
		require.NotEqual(t, keyID, newKeyID)

		// use manager to decrypt the data.
		decryptedData, err := m.Decrypt(cipher, nonce, keyID)
		require.Nil(t, err)

		assert.Equal(t, testData, decryptedData)
	},
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

var buf = make([]byte, 8192)

func BenchmarkEncryption(b *testing.B) {
	benchEncrypt(b, 1024)
	benchEncrypt(b, 4096)
	benchEncrypt(b, 8192)
}

func BenchmarkDecryption(b *testing.B) {
	benchDecrypt(b, 1024)
	benchDecrypt(b, 4096)
	benchDecrypt(b, 8192)
}

func benchEncrypt(b *testing.B, size int) {
	m, err := NewManager()
	if err != nil {
		b.Fatal("failed to create manager", err)
	}
	// disable auto rotation to avoid skewing results.
	maxWriteCount = math.MaxInt32

	b.Run(fmt.Sprintf("encrypt-%d", size), func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(size))
		for i := 0; i < b.N; i++ {
			_, _, _, err := m.Encrypt(buf[:size])
			if err != nil {
				b.Fatal("error encrypting data", err)
			}
		}
	})
}

func benchDecrypt(b *testing.B, size int) {
	m, err := NewManager()
	if err != nil {
		b.Fatal("failed to create manager", err)
	}

	edata, enonce, kid, err := m.Encrypt(buf[:size])
	if err != nil {
		b.Fatal("failed to encrypt data", err)
	}

	b.Run(fmt.Sprintf("decrypt-%d", size), func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(size))
		for i := 0; i < b.N; i++ {
			_, err := m.Decrypt(edata, enonce, kid)
			if err != nil {
				b.Fatal("error encrypting data", err)
			}
		}
	})
}
