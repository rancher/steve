package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/rancher/steve/pkg/sqlcache/encryption"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Mocks for this test are generated with the following command.
//go:generate mockgen --build_flags=--mod=mod -package db -destination ./db_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db Rows,Connection,Encryptor,Decryptor,TxClient,Stmt

type testStoreObject struct {
	Id  string
	Val string
}

func TestNewClient(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Query rows with no params, no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		expectedClient := &client{
			conn:      c,
			encryptor: e,
			decryptor: d,
			encoding:  defaultEncoding,
		}
		client, _, err := NewClient(c, e, d, false)
		assert.Nil(t, err)
		assert.Equal(t, expectedClient, client)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestQueryForRows(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Query rows with no params, no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		client := SetupClient(t, c, nil, nil)
		s := NewMockStmt(gomock.NewController(t))
		ctx := context.Background()
		r := &sql.Rows{}
		s.EXPECT().QueryContext(ctx).Return(r, nil)
		rows, err := client.QueryForRows(ctx, s)
		assert.Nil(t, err)
		assert.Equal(t, r, rows)
	},
	})
	tests = append(tests, testCase{description: "Query rows with params, QueryContext() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		client := SetupClient(t, c, nil, nil)
		s := NewMockStmt(gomock.NewController(t))
		ctx := context.Background()
		s.EXPECT().QueryContext(ctx).Return(nil, fmt.Errorf("error"))
		_, err := client.QueryForRows(ctx, s)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestQueryObjects(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	testObject := &testStoreObject{Id: "something", Val: "a"}
	var keyId uint32 = math.MaxUint32
	fmt.Println(reflect.TypeOf(testObject).Name())

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Query objects, with one row, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			*a[0].(*sql.RawBytes) = toBytes(testObject)
			*a[1].(*sql.RawBytes) = toBytes(testObject)
			*a[2].(*uint32) = keyId
		})
		d.EXPECT().Decrypt(toBytes(testObject), toBytes(testObject), keyId).Return(toBytes(testObject), nil)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(items))
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and a decrypt error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			*a[0].(*sql.RawBytes) = toBytes(testObject)
			*a[1].(*sql.RawBytes) = toBytes(
				testObject)
			*a[2].(*uint32) = keyId
		})
		d.EXPECT().Decrypt(toBytes(testObject), toBytes(testObject), keyId).Return(nil, fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and a Scan() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and a Close() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			*a[0].(*sql.RawBytes) = toBytes(testObject)
			*a[1].(*sql.RawBytes) = toBytes(testObject)
			*a[2].(*uint32) = keyId
		})
		d.EXPECT().Decrypt(toBytes(testObject), toBytes(testObject), keyId).Return(toBytes(testObject), nil)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(fmt.Errorf("error"))
		client := SetupClient(t, c, e, d)
		_, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Query objects, with no rows, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(items))
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestQueryStrings(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	testObject := testStoreObject{Id: "something", Val: "a"}
	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "ReadStrings(), with one row, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*string)
				*vk = string(toBytes(testObject.Id))
			}
		})
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadStrings(r)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(items))
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and Scan error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadStrings(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "ReadStrings(), with one row, and Err() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*string)
				*vk = string(toBytes(testObject.Id))
			}
		})
		r.EXPECT().Next().Return(false)
		r.EXPECT().Err().Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadStrings(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "ReadStrings(), with one row, and Close() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*string)
				*vk = string(toBytes(testObject.Id))
			}
		})
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(fmt.Errorf("error"))
		client := SetupClient(t, c, e, d)
		_, err := client.ReadStrings(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "ReadStrings(), with no rows, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadStrings(r)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(items))
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestReadInt(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	testResult := 42
	tests = append(tests, testCase{description: "One row, no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			p := a[0].(*int)
			*p = testResult
		})
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		result, err := client.ReadInt(r)
		assert.Nil(t, err)
		assert.Equal(t, 42, result)
	},
	})
	tests = append(tests, testCase{description: "One row, Scan error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadInt(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "One row, Err() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			a[0] = testResult
		})
		r.EXPECT().Err().Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadInt(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "One row, Close() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			a[0] = testResult
		})
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Close().Return(fmt.Errorf("error"))
		client := SetupClient(t, c, e, d)
		_, err := client.ReadInt(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "No rows error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadInt(r)
		assert.ErrorIs(t, err, sql.ErrNoRows)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestUpsert(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	testObject := testStoreObject{Id: "something", Val: "a"}
	var keyID uint32 = 5

	// Tests with shouldEncryptSet to true
	tests = append(tests, testCase{description: "Upsert() with no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		txC := NewMockTxClient(gomock.NewController(t))
		stmt := NewMockStmt(gomock.NewController(t))
		testObjBytes := toBytes(testObject)
		testByteValue := []byte("something")
		e.EXPECT().Encrypt(testObjBytes).Return(testByteValue, testByteValue, keyID, nil)
		txC.EXPECT().Stmt(stmt).Return(stmt)
		stmt.EXPECT().Exec("somekey", testByteValue, testByteValue, keyID).Return(nil, nil)
		err := client.Upsert(txC, stmt, "somekey", testObject, true)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Upsert() with Encrypt() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		txC := NewMockTxClient(gomock.NewController(t))
		sqlStmt := &stmt{}
		testObjBytes := toBytes(testObject)
		e.EXPECT().Encrypt(testObjBytes).Return(nil, nil, uint32(0), fmt.Errorf("error"))
		err := client.Upsert(txC, sqlStmt, "somekey", testObject, true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Upsert() with StmtExec() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		txC := NewMockTxClient(gomock.NewController(t))
		stmt := NewMockStmt(gomock.NewController(t))
		testObjBytes := toBytes(testObject)
		testByteValue := []byte("something")
		e.EXPECT().Encrypt(testObjBytes).Return(testByteValue, testByteValue, keyID, nil)
		txC.EXPECT().Stmt(stmt).Return(stmt)
		stmt.EXPECT().Exec("somekey", testByteValue, testByteValue, keyID).Return(nil, fmt.Errorf("error"))
		err := client.Upsert(txC, stmt, "somekey", testObject, true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Upsert() with no errors and shouldEncrypt false", test: func(t *testing.T) {
		c := SetupMockConnection(t)

		// nil encryptor/decryptor
		client := SetupClient(t, c, nil, nil)
		txC := NewMockTxClient(gomock.NewController(t))
		stmt := NewMockStmt(gomock.NewController(t))
		var testByteValue []byte
		testObjBytes := toBytes(testObject)
		txC.EXPECT().Stmt(stmt).Return(stmt)
		stmt.EXPECT().Exec("somekey", testObjBytes, testByteValue, uint32(0)).Return(nil, nil)
		err := client.Upsert(txC, stmt, "somekey", testObject, false)
		assert.Nil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestPrepare(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "Prepare() with no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		sqlStmt := &sql.Stmt{}
		c.EXPECT().Prepare("something").Return(sqlStmt, nil)

		stmt := client.Prepare("something")
		assert.Equal(t, sqlStmt, stmt.SQLStmt())
		assert.Equal(t, "something", stmt.GetQueryString())
	},
	})
	tests = append(tests, testCase{description: "Prepare() with Connection Prepare() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		c.EXPECT().Prepare("something").Return(nil, fmt.Errorf("error"))

		assert.Panics(t, func() { client.Prepare("something") })
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestNewConnection(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "NewConnection replaces file", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		c.EXPECT().Close().Return(nil)

		dbPath, err := client.NewConnection(true)
		assert.Nil(t, err)

		// Create a transaction to ensure that the file is written to disk.
		err = client.WithTransaction(context.Background(), false, func(tx TxClient) error {
			return nil
		})
		assert.NoError(t, err)

		assert.FileExists(t, dbPath)
		assertFileHasPermissions(t, dbPath, 0600)

		err = os.Remove(dbPath)
		if err != nil {
			assert.Fail(t, "could not remove object cache path after test")
		}
	},
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestCommit(t *testing.T) {

}

func TestRollback(t *testing.T) {

}

func SetupMockConnection(t *testing.T) *MockConnection {
	mockC := NewMockConnection(gomock.NewController(t))
	return mockC
}

func SetupMockEncryptor(t *testing.T) *MockEncryptor {
	mockE := NewMockEncryptor(gomock.NewController(t))
	return mockE
}

func SetupMockDecryptor(t *testing.T) *MockDecryptor {
	MockD := NewMockDecryptor(gomock.NewController(t))
	return MockD
}

func SetupMockRows(t *testing.T) *MockRows {
	MockR := NewMockRows(gomock.NewController(t))
	return MockR
}

func SetupClient(t *testing.T, connection Connection, encryptor Encryptor, decryptor Decryptor) Client {
	t.Helper()
	// No need to specify temp dir for this client because the connection is mocked
	c, _, err := NewClient(connection, encryptor, decryptor, false)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestTouchFile(t *testing.T) {
	t.Run("File doesn't exist before", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "test1.txt")
		assert.NoError(t, touchFile(filename, 0600))
		assertFileHasPermissions(t, filename, 0600)
	})

	t.Run("File exists with different permissions", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "test2.txt")
		assert.NoError(t, os.WriteFile(filename, []byte("test"), 0644))
		assert.NoError(t, touchFile(filename, 0600))
		assertFileHasPermissions(t, filename, 0600)
	})
}

func assertFileHasPermissions(t *testing.T, fname string, wantPerms fs.FileMode) bool {
	t.Helper()
	info, err := os.Lstat(fname)
	if err != nil {
		if os.IsNotExist(err) {
			return assert.Fail(t, fmt.Sprintf("unable to find file %q", fname))
		}
		return assert.Fail(t, fmt.Sprintf("error when running os.Lstat(%q): %s", fname, err))
	}

	// Stringifying the perms makes it easier to read than a Hex comparison.
	assert.Equal(t, wantPerms.String(), info.Mode().Perm().String())

	return true
}

func toBytes(obj any) []byte {
	var buf bytes.Buffer
	if err := defaultEncoding.Encode(&buf, obj); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func Test_client_serialization(t *testing.T) {
	testObject := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "test",
			Annotations: map[string]string{"annotation": "test"},
			Labels:      map[string]string{"label": "test"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "test",
				Image: "testimage",
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "test",
					MountPath: "/test",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "test",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}},
		},
	}
	tests := []struct {
		name     string
		encoding Encoding
	}{
		{name: "gob", encoding: GobEncoding},
		{name: "json", encoding: JSONEncoding},
		{name: "json+gzip", encoding: GzippedJSONEncoding},
		{name: "gob+gzip", encoding: GzippedGobEncoding},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm, err := encryption.NewManager()
			if err != nil {
				t.Fatal(err)
			}
			c := &client{
				encryptor: cm, decryptor: cm,
			}
			WithEncoding(tt.encoding)(c)
			serialized, err := c.Serialize(testObject, false)
			if err != nil {
				t.Fatal(err)
			}
			deserialized, err := c.Deserialize(serialized, reflect.TypeOf(testObject))
			if err != nil {
				t.Fatal(err)
			}
			got, ok := deserialized.(*corev1.Pod)
			if !ok {
				t.Errorf("deserialized object is not *corev1.Pod")
			} else if diff := cmp.Diff(testObject, got); diff != "" {
				t.Errorf("Deserialize(...): -want, +got:\n%s", diff)
			}
		})
	}
}
