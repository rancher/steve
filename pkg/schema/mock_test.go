package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"

	"github.com/rancher/steve/pkg/accesscontrol"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

const (
	insideSeparator  = "&"
	outsideSeparator = "%"
)

type mockAccessSetLookup struct {
	accessSets  map[string]*accesscontrol.AccessSet
	currentHash map[string]hash.Hash
}

func newMockAccessSetLookup() *mockAccessSetLookup {
	return &mockAccessSetLookup{
		accessSets:  map[string]*accesscontrol.AccessSet{},
		currentHash: map[string]hash.Hash{},
	}
}

func (m *mockAccessSetLookup) AccessFor(user user.Info) *accesscontrol.AccessSet {
	if set, ok := m.accessSets[user.GetName()]; ok {
		return set
	}
	return nil
}

func (m *mockAccessSetLookup) PurgeUserData(id string) {
	var foundKey string
	for key, value := range m.accessSets {
		if value.ID == id {
			foundKey = key
		}
	}
	if foundKey != "" {
		delete(m.accessSets, foundKey)
	}
}

func (m *mockAccessSetLookup) AddAccessForUser(user user.Info, verb string, gr schema.GroupResource, namespace string, name string) {
	currentAccessSet, ok := m.accessSets[user.GetName()]
	var currentHash hash.Hash
	if !ok {
		currentAccessSet = &accesscontrol.AccessSet{}
		currentHash = sha256.New()
	} else {
		currentHash = m.currentHash[currentAccessSet.ID]
	}
	currentAccessSet.Add(verb, gr, accesscontrol.Access{Namespace: namespace, ResourceName: name})
	calculateAccessSetID(currentHash, verb, gr, namespace, name)
	currentAccessSet.ID = hex.EncodeToString(currentHash.Sum(nil))
	m.accessSets[user.GetName()] = currentAccessSet
	m.currentHash[currentAccessSet.ID] = currentHash
}

func (m *mockAccessSetLookup) Clear() {
	m.accessSets = map[string]*accesscontrol.AccessSet{}
	m.currentHash = map[string]hash.Hash{}
}

func calculateAccessSetID(digest hash.Hash, verb string, gr schema.GroupResource, namespace string, name string) {
	digest.Write([]byte(verb + insideSeparator))
	digest.Write([]byte(gr.String() + insideSeparator))
	digest.Write([]byte(namespace + insideSeparator))
	digest.Write([]byte(name + outsideSeparator))
}
