package partition

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/wrangler/v2/pkg/generic"
	"github.com/rancher/wrangler/v2/pkg/schemas"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

func TestList(t *testing.T) {
	tests := []struct {
		name          string
		apiOps        []*types.APIRequest
		access        []map[string]string
		partitions    map[string][]Partition
		objects       map[string]*unstructured.UnstructuredList
		want          []types.APIObjectList
		wantCache     []mockCache
		disableCache  bool
		wantListCalls []map[string]int
	}{
		{
			name: "basic",
			apiOps: []*types.APIRequest{
				newRequest("", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
			},
		},
		{
			name: "limit and continue",
			apiOps: []*types.APIRequest{
				newRequest("limit=1", "user1"),
				newRequest(fmt.Sprintf("limit=1&continue=%s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("granny-smith")))))), "user1"),
				newRequest(fmt.Sprintf("limit=1&continue=%s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("crispin")))))), "user1"),
				newRequest("limit=-1", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    1,
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("granny-smith"))))),
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count:    1,
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"p":"all","c":"%s","l":1}`, base64.StdEncoding.EncodeToString([]byte("crispin"))))),
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
					},
				},
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("granny-smith").toObj(),
						newApple("crispin").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition",
			apiOps: []*types.APIRequest{
				newRequest("", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "green",
					},
					mockPartition{
						name: "yellow",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 2,
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("crispin").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition with limit and continue",
			apiOps: []*types.APIRequest{
				newRequest("limit=3", "user1"),
				newRequest(fmt.Sprintf("limit=3&continue=%s", base64.StdEncoding.EncodeToString([]byte(`{"p":"green","o":1,"l":3}`))), "user1"),
				newRequest(fmt.Sprintf("limit=3&continue=%s", base64.StdEncoding.EncodeToString([]byte(`{"p":"red","l":3}`))), "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "pink",
					},
					mockPartition{
						name: "green",
					},
					mockPartition{
						name: "yellow",
					},
					mockPartition{
						name: "red",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("honeycrisp").Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
						newApple("golden-delicious").Unstructured,
					},
				},
				"red": {
					Items: []unstructured.Unstructured{
						newApple("red-delicious").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    3,
					Continue: base64.StdEncoding.EncodeToString([]byte(`{"p":"green","o":1,"l":3}`)),
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("honeycrisp").toObj(),
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    3,
					Continue: base64.StdEncoding.EncodeToString([]byte(`{"p":"red","l":3}`)),
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
						newApple("crispin").toObj(),
						newApple("golden-delicious").toObj(),
					},
				},
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("red-delicious").toObj(),
					},
				},
			},
		},
		{
			name: "with filters",
			apiOps: []*types.APIRequest{
				newRequest("filter=data.color=green", "user1"),
				newRequest("filter=data.color=green&filter=metadata.name=bramley", "user1"),
				newRequest("filter=data.color=green,data.color=pink", "user1"),
				newRequest("filter=data.color=green,data.color=pink&filter=metadata.name=fuji", "user1"),
				newRequest("filter=data.color=green,data.color=pink&filter=metadata.name=crispin", "user1"),
				newRequest("filter=data.color!=green", "user1"),
				newRequest("filter=data.color!=green,metadata.name=granny-smith", "user1"),
				newRequest("filter=data.color!=green&filter=metadata.name!=crispin", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 2,
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("bramley").toObj(),
					},
				},
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
					},
				},
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("granny-smith").toObj(),
						newApple("bramley").toObj(),
					},
				},
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count: 0,
				},
				{
					Count: 2,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("crispin").toObj(),
					},
				},
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("granny-smith").toObj(),
						newApple("crispin").toObj(),
					},
				},
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition with filters",
			apiOps: []*types.APIRequest{
				newRequest("filter=data.category=baking", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "pink",
					},
					mockPartition{
						name: "green",
					},
					mockPartition{
						name: "yellow",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").with(map[string]string{"category": "eating"}).Unstructured,
						newApple("honeycrisp").with(map[string]string{"category": "eating,baking"}).Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").with(map[string]string{"category": "baking"}).Unstructured,
						newApple("bramley").with(map[string]string{"category": "eating"}).Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").with(map[string]string{"category": "baking"}).Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("honeycrisp").with(map[string]string{"category": "eating,baking"}).toObj(),
						newApple("granny-smith").with(map[string]string{"category": "baking"}).toObj(),
						newApple("crispin").with(map[string]string{"category": "baking"}).toObj(),
					},
				},
			},
		},
		{
			name: "with sorting",
			apiOps: []*types.APIRequest{
				newRequest("sort=metadata.name", "user1"),
				newRequest("sort=-metadata.name", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 4,
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
						newApple("crispin").toObj(),
						newApple("fuji").toObj(),
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count: 4,
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("fuji").toObj(),
						newApple("crispin").toObj(),
						newApple("bramley").toObj(),
					},
				},
			},
		},
		{
			name: "sorting with secondary sort",
			apiOps: []*types.APIRequest{
				newRequest("sort=data.color,metadata.name,", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("honeycrisp").Unstructured,
						newApple("granny-smith").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
						newApple("fuji").toObj(),
						newApple("honeycrisp").toObj(),
					},
				},
			},
		},
		{
			name: "sorting with missing primary sort is unsorted",
			apiOps: []*types.APIRequest{
				newRequest("sort=,metadata.name", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("honeycrisp").Unstructured,
						newApple("granny-smith").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("honeycrisp").toObj(),
						newApple("granny-smith").toObj(),
					},
				},
			},
		},
		{
			name: "sorting with missing secondary sort is single-column sorted",
			apiOps: []*types.APIRequest{
				newRequest("sort=metadata.name,", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("honeycrisp").Unstructured,
						newApple("granny-smith").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
						newApple("granny-smith").toObj(),
						newApple("honeycrisp").toObj(),
					},
				},
			},
		},
		{
			name: "multi-partition sort=metadata.name",
			apiOps: []*types.APIRequest{
				newRequest("sort=metadata.name", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "green",
					},
					mockPartition{
						name: "yellow",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
				"green": {
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
					},
				},
				"yellow": {
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 2,
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
						newApple("granny-smith").toObj(),
					},
				},
			},
		},
		{
			name: "pagination",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1", "user1"),
				newRequest("pagesize=1&page=2&revision=42", "user1"),
				newRequest("pagesize=1&page=3&revision=42", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "42",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"all": 1},
				{"all": 1},
				{"all": 1},
			},
		},
		{
			name: "access-change pagination",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1", "user1"),
				newRequest("pagesize=1&page=2&revision=42", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleB",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "42",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleB"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"all": 1},
				{"all": 2},
			},
		},
		{
			name: "pagination with cache disabled",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1", "user1"),
				newRequest("pagesize=1&page=2&revision=42", "user1"),
				newRequest("pagesize=1&page=3&revision=42", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "42",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
				},
			},
			wantCache:    []mockCache{},
			disableCache: true,
			wantListCalls: []map[string]int{
				{"all": 1},
				{"all": 2},
				{"all": 3},
			},
		},
		{
			name: "multi-partition pagesize=1",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1", "user1"),
				newRequest("pagesize=1&page=2&revision=102", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "green",
					},
					mockPartition{
						name: "yellow",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "101",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
				"green": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "102",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
					},
				},
				"yellow": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "103",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "102",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "102",
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
					},
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("crispin").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("crispin").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"green": 1, "yellow": 1},
				{"green": 1, "yellow": 1},
			},
		},
		{
			name: "pagesize=1 & limit=2 & continue",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1&limit=2", "user1"),
				newRequest("pagesize=1&page=2&limit=2", "user1"),             // does not use cache
				newRequest("pagesize=1&page=2&revision=42&limit=2", "user1"), // uses cache
				newRequest("pagesize=1&page=3&revision=42&limit=2", "user1"), // next page from cache
				newRequest(fmt.Sprintf("pagesize=1&revision=42&limit=2&continue=%s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`)))))), "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "42",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("crispin").Unstructured,
						newApple("red-delicious").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Continue: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
					},
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    2,
							resume:       "",
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Object: map[string]interface{}{
								"metadata": map[string]interface{}{
									"continue": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
								},
							},
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    2,
							resume:       "",
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Object: map[string]interface{}{
								"metadata": map[string]interface{}{
									"continue": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
								},
							},
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    2,
							resume:       "",
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Object: map[string]interface{}{
								"metadata": map[string]interface{}{
									"continue": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
								},
							},
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    2,
							resume:       "",
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Object: map[string]interface{}{
								"metadata": map[string]interface{}{
									"continue": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
								},
							},
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    2,
							resume:       "",
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Object: map[string]interface{}{
								"metadata": map[string]interface{}{
									"continue": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
								},
							},
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
						{
							chunkSize:    2,
							resume:       base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"r":"42","p":"all","c":"%s","l":2}`, base64.StdEncoding.EncodeToString([]byte(`crispin`))))),
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("crispin").Unstructured,
								newApple("red-delicious").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"all": 2},
				{"all": 4},
				{"all": 4},
				{"all": 4},
				{"all": 5},
			},
		},
		{
			name: "multi-user pagination",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1", "user1"),
				newRequest("pagesize=1", "user2"),
				newRequest("pagesize=1&page=2&revision=42", "user1"),
				newRequest("pagesize=1&page=2&revision=42", "user2"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user2": "roleB",
				},
				{
					"user1": "roleA",
				},
				{
					"user2": "roleB",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
				"user2": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "42",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("fuji").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user2", "roleB"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user2", "roleB"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user2", "roleB"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"all": 1},
				{"all": 2},
				{"all": 2},
				{"all": 2},
			},
		},
		{
			name: "multi-partition multi-user pagination",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1", "user1"),
				newRequest("pagesize=1", "user2"),
				newRequest("pagesize=1&page=2&revision=102", "user1"),
				newRequest("pagesize=1&page=2&revision=103", "user2"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user2": "roleB",
				},
				{
					"user1": "roleA",
				},
				{
					"user2": "roleB",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "green",
					},
				},
				"user2": {
					mockPartition{
						name: "yellow",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "101",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
				"green": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "102",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
					},
				},
				"yellow": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "103",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
						newApple("golden-delicious").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "102",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "103",
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "102",
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "103",
					Objects: []types.APIObject{
						newApple("golden-delicious").toObj(),
					},
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						cacheKey{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("bramley").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("bramley").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user2", "roleB"),
							resourcePath: "/apples",
							revision:     "103",
						}: {
							Items: []unstructured.Unstructured{
								newApple("crispin").Unstructured,
								newApple("golden-delicious").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("bramley").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user2", "roleB"),
							resourcePath: "/apples",
							revision:     "103",
						}: {
							Items: []unstructured.Unstructured{
								newApple("crispin").Unstructured,
								newApple("golden-delicious").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("bramley").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user2", "roleB"),
							resourcePath: "/apples",
							revision:     "103",
						}: {
							Items: []unstructured.Unstructured{
								newApple("crispin").Unstructured,
								newApple("golden-delicious").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"green": 1, "yellow": 0},
				{"green": 1, "yellow": 1},
				{"green": 1, "yellow": 1},
				{"green": 1, "yellow": 1},
			},
		},
		{
			name: "multi-partition access-change pagination",
			apiOps: []*types.APIRequest{
				newRequest("pagesize=1", "user1"),
				newRequest("pagesize=1&page=2&revision=102", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleB",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "green",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"pink": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "101",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
					},
				},
				"green": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "102",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("granny-smith").Unstructured,
						newApple("bramley").Unstructured,
					},
				},
				"yellow": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "103",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("crispin").Unstructured,
						newApple("golden-delicious").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    2,
					Pages:    2,
					Revision: "102",
					Objects: []types.APIObject{
						newApple("granny-smith").toObj(),
					},
				},
				{
					Count:    2,
					Pages:    2,
					Revision: "102",
					Objects: []types.APIObject{
						newApple("bramley").toObj(),
					},
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						cacheKey{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("bramley").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("bramley").Unstructured,
							},
						},
						{
							chunkSize:    100000,
							pageSize:     1,
							accessID:     getAccessID("user1", "roleB"),
							resourcePath: "/apples",
							revision:     "102",
						}: {
							Items: []unstructured.Unstructured{
								newApple("granny-smith").Unstructured,
								newApple("bramley").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"green": 1},
				{"green": 2},
			},
		},
		{
			name: "pagination with or filters",
			apiOps: []*types.APIRequest{
				newRequest("filter=metadata.name=el,data.color=el&pagesize=2", "user1"),
				newRequest("filter=metadata.name=el,data.color=el&pagesize=2&page=2&revision=42", "user1"),
				newRequest("filter=metadata.name=el,data.color=el&pagesize=2&page=3&revision=42", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"resourceVersion": "42",
						},
					},
					Items: []unstructured.Unstructured{
						newApple("fuji").Unstructured,
						newApple("granny-smith").Unstructured,
						newApple("red-delicious").Unstructured,
						newApple("golden-delicious").Unstructured,
						newApple("crispin").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count:    3,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("red-delicious").toObj(),
						newApple("golden-delicious").toObj(),
					},
				},
				{
					Count:    3,
					Pages:    2,
					Revision: "42",
					Objects: []types.APIObject{
						newApple("crispin").toObj(),
					},
				},
				{
					Count:    3,
					Pages:    2,
					Revision: "42",
				},
			},
			wantCache: []mockCache{
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							filters:      "data.color=el,metadata.name=el",
							pageSize:     2,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("red-delicious").Unstructured,
								newApple("golden-delicious").Unstructured,
								newApple("crispin").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							filters:      "data.color=el,metadata.name=el",
							pageSize:     2,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("red-delicious").Unstructured,
								newApple("golden-delicious").Unstructured,
								newApple("crispin").Unstructured,
							},
						},
					},
				},
				{
					contents: map[cacheKey]*unstructured.UnstructuredList{
						{
							chunkSize:    100000,
							filters:      "data.color=el,metadata.name=el",
							pageSize:     2,
							accessID:     getAccessID("user1", "roleA"),
							resourcePath: "/apples",
							revision:     "42",
						}: {
							Items: []unstructured.Unstructured{
								newApple("red-delicious").Unstructured,
								newApple("golden-delicious").Unstructured,
								newApple("crispin").Unstructured,
							},
						},
					},
				},
			},
			wantListCalls: []map[string]int{
				{"all": 1},
				{"all": 1},
				{"all": 1},
			},
		},
		{
			name: "with project filters",
			apiOps: []*types.APIRequest{
				newRequest("projectsornamespaces=p-abcde", "user1"),
				newRequest("projectsornamespaces=p-abcde,p-fghij", "user1"),
				newRequest("projectsornamespaces=p-abcde,n2", "user1"),
				newRequest("projectsornamespaces!=p-abcde", "user1"),
				newRequest("projectsornamespaces!=p-abcde,p-fghij", "user1"),
				newRequest("projectsornamespaces!=p-abcde,n2", "user1"),
				newRequest("projectsornamespaces=foobar", "user1"),
				newRequest("projectsornamespaces!=foobar", "user1"),
			},
			access: []map[string]string{
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
				{
					"user1": "roleA",
				},
			},
			partitions: map[string][]Partition{
				"user1": {
					mockPartition{
						name: "all",
					},
				},
			},
			objects: map[string]*unstructured.UnstructuredList{
				"all": {
					Items: []unstructured.Unstructured{
						newApple("fuji").withNamespace("n1").Unstructured,
						newApple("granny-smith").withNamespace("n1").Unstructured,
						newApple("bramley").withNamespace("n2").Unstructured,
						newApple("crispin").withNamespace("n3").Unstructured,
					},
				},
			},
			want: []types.APIObjectList{
				{
					Count: 2,
					Objects: []types.APIObject{
						newApple("fuji").withNamespace("n1").toObj(),
						newApple("granny-smith").withNamespace("n1").toObj(),
					},
				},
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("fuji").withNamespace("n1").toObj(),
						newApple("granny-smith").withNamespace("n1").toObj(),
						newApple("bramley").withNamespace("n2").toObj(),
					},
				},
				{
					Count: 3,
					Objects: []types.APIObject{
						newApple("fuji").withNamespace("n1").toObj(),
						newApple("granny-smith").withNamespace("n1").toObj(),
						newApple("bramley").withNamespace("n2").toObj(),
					},
				},
				{
					Count: 2,
					Objects: []types.APIObject{
						newApple("bramley").withNamespace("n2").toObj(),
						newApple("crispin").withNamespace("n3").toObj(),
					},
				},
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("crispin").withNamespace("n3").toObj(),
					},
				},
				{
					Count: 1,
					Objects: []types.APIObject{
						newApple("crispin").withNamespace("n3").toObj(),
					},
				},
				{
					Count: 0,
				},
				{
					Count: 4,
					Objects: []types.APIObject{
						newApple("fuji").withNamespace("n1").toObj(),
						newApple("granny-smith").withNamespace("n1").toObj(),
						newApple("bramley").withNamespace("n2").toObj(),
						newApple("crispin").withNamespace("n3").toObj(),
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := &types.APISchema{Schema: &schemas.Schema{ID: "apple"}}
			stores := map[string]UnstructuredStore{}
			for _, partitions := range test.partitions {
				for _, p := range partitions {
					stores[p.Name()] = &mockStore{
						contents: test.objects[p.Name()],
					}
				}
			}
			asl := &mockAccessSetLookup{userRoles: test.access}
			if !test.disableCache {
				t.Setenv("CATTLE_REQUEST_CACHE_DISABLED", "false")
			}
			store := NewStore(mockPartitioner{
				stores:     stores,
				partitions: test.partitions,
			}, asl, mockNamespaceCache{})
			for i, req := range test.apiOps {
				got, gotErr := store.List(req, schema)
				assert.Nil(t, gotErr)
				assert.Equal(t, test.want[i], got)
				if test.disableCache {
					assert.Nil(t, store.listCache)
				}
				if len(test.wantCache) > 0 {
					assert.Equal(t, len(test.wantCache[i].contents), len(store.listCache.Keys()))
					for k, v := range test.wantCache[i].contents {
						cachedVal, _ := store.listCache.Get(k)
						assert.Equal(t, v, cachedVal)
					}
				}
				if len(test.wantListCalls) > 0 {
					for name, _ := range store.Partitioner.(mockPartitioner).stores {
						assert.Equal(t, test.wantListCalls[i][name], store.Partitioner.(mockPartitioner).stores[name].(*mockStore).called)
					}
				}
			}
		})
	}
}

func TestListByRevision(t *testing.T) {

	schema := &types.APISchema{Schema: &schemas.Schema{ID: "apple"}}
	asl := &mockAccessSetLookup{userRoles: []map[string]string{
		{
			"user1": "roleA",
		},
		{
			"user1": "roleA",
		},
	}}
	store := NewStore(mockPartitioner{
		stores: map[string]UnstructuredStore{
			"all": &mockVersionedStore{
				versions: []mockStore{
					{
						contents: &unstructured.UnstructuredList{
							Object: map[string]interface{}{
								"metadata": map[string]interface{}{
									"resourceVersion": "1",
								},
							},
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
							},
						},
					},
					{
						contents: &unstructured.UnstructuredList{
							Object: map[string]interface{}{
								"metadata": map[string]interface{}{
									"resourceVersion": "2",
								},
							},
							Items: []unstructured.Unstructured{
								newApple("fuji").Unstructured,
								newApple("granny-smith").Unstructured,
							},
						},
					},
				},
			},
		},
		partitions: map[string][]Partition{
			"user1": {
				mockPartition{
					name: "all",
				},
			},
		},
	}, asl, mockNamespaceCache{})
	req := newRequest("", "user1")

	got, gotErr := store.List(req, schema)
	assert.Nil(t, gotErr)
	wantVersion := "2"
	assert.Equal(t, wantVersion, got.Revision)

	req = newRequest("revision=1", "user1")
	got, gotErr = store.List(req, schema)
	assert.Nil(t, gotErr)
	wantVersion = "1"
	assert.Equal(t, wantVersion, got.Revision)
}

type mockPartitioner struct {
	stores     map[string]UnstructuredStore
	partitions map[string][]Partition
}

func (m mockPartitioner) Lookup(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) (Partition, error) {
	panic("not implemented")
}

func (m mockPartitioner) All(apiOp *types.APIRequest, schema *types.APISchema, verb, id string) ([]Partition, error) {
	user, _ := request.UserFrom(apiOp.Request.Context())
	return m.partitions[user.GetName()], nil
}

func (m mockPartitioner) Store(apiOp *types.APIRequest, partition Partition) (UnstructuredStore, error) {
	return m.stores[partition.Name()], nil
}

type mockPartition struct {
	name string
}

func (m mockPartition) Name() string {
	return m.name
}

type mockStore struct {
	contents  *unstructured.UnstructuredList
	partition mockPartition
	called    int
}

func (m *mockStore) List(apiOp *types.APIRequest, schema *types.APISchema) (*unstructured.UnstructuredList, []types.Warning, error) {
	m.called++
	query, _ := url.ParseQuery(apiOp.Request.URL.RawQuery)
	l := query.Get("limit")
	if l == "" {
		return m.contents, nil, nil
	}
	i := 0
	if c := query.Get("continue"); c != "" {
		start, _ := base64.StdEncoding.DecodeString(c)
		for j, obj := range m.contents.Items {
			if string(start) == obj.GetName() {
				i = j
				break
			}
		}
	}
	lInt, _ := strconv.Atoi(l)
	contents := m.contents.DeepCopy()
	if len(contents.Items) > i+lInt {
		contents.SetContinue(base64.StdEncoding.EncodeToString([]byte(contents.Items[i+lInt].GetName())))
	}
	if i > len(contents.Items) {
		return contents, nil, nil
	}
	if i+lInt > len(contents.Items) {
		contents.Items = contents.Items[i:]
		return contents, nil, nil
	}
	contents.Items = contents.Items[i : i+lInt]
	return contents, nil, nil
}

func (m *mockStore) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (*unstructured.Unstructured, []types.Warning, error) {
	panic("not implemented")
}

func (m *mockStore) Create(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject) (*unstructured.Unstructured, []types.Warning, error) {
	panic("not implemented")
}

func (m *mockStore) Update(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject, id string) (*unstructured.Unstructured, []types.Warning, error) {
	panic("not implemented")
}

func (m *mockStore) Delete(apiOp *types.APIRequest, schema *types.APISchema, id string) (*unstructured.Unstructured, []types.Warning, error) {
	panic("not implemented")
}

func (m *mockStore) Watch(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest) (chan watch.Event, error) {
	panic("not implemented")
}

type mockVersionedStore struct {
	mockStore
	versions []mockStore
}

func (m *mockVersionedStore) List(apiOp *types.APIRequest, schema *types.APISchema) (*unstructured.UnstructuredList, []types.Warning, error) {
	m.called++
	query, _ := url.ParseQuery(apiOp.Request.URL.RawQuery)
	rv := len(m.versions) - 1
	if query.Get("resourceVersion") != "" {
		rv, _ = strconv.Atoi(query.Get("resourceVersion"))
		rv--
	}
	l := query.Get("limit")
	if l == "" {
		return m.versions[rv].contents, nil, nil
	}
	i := 0
	if c := query.Get("continue"); c != "" {
		start, _ := base64.StdEncoding.DecodeString(c)
		for j, obj := range m.versions[rv].contents.Items {
			if string(start) == obj.GetName() {
				i = j
				break
			}
		}
	}
	lInt, _ := strconv.Atoi(l)
	contents := m.versions[rv].contents.DeepCopy()
	if len(contents.Items) > i+lInt {
		contents.SetContinue(base64.StdEncoding.EncodeToString([]byte(contents.Items[i+lInt].GetName())))
	}
	if i > len(contents.Items) {
		return contents, nil, nil
	}
	if i+lInt > len(contents.Items) {
		contents.Items = contents.Items[i:]
		return contents, nil, nil
	}
	contents.Items = contents.Items[i : i+lInt]
	return contents, nil, nil
}

type mockCache struct {
	contents map[cacheKey]*unstructured.UnstructuredList
}

var colorMap = map[string]string{
	"fuji":             "pink",
	"honeycrisp":       "pink",
	"granny-smith":     "green",
	"bramley":          "green",
	"crispin":          "yellow",
	"golden-delicious": "yellow",
	"red-delicious":    "red",
}

func newRequest(query, username string) *types.APIRequest {
	return &types.APIRequest{
		Request: (&http.Request{
			URL: &url.URL{
				Scheme:   "https",
				Host:     "rancher",
				Path:     "/apples",
				RawQuery: query,
			},
		}).WithContext(request.WithUser(context.Background(), &user.DefaultInfo{
			Name:   username,
			Groups: []string{"system:authenticated"},
		})),
	}
}

type apple struct {
	unstructured.Unstructured
}

func newApple(name string) apple {
	return apple{unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind": "apple",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"data": map[string]interface{}{
				"color": colorMap[name],
			},
		},
	}}
}

func (a apple) toObj() types.APIObject {
	meta := a.Object["metadata"].(map[string]interface{})
	id := meta["name"].(string)
	ns, ok := meta["namespace"]
	if ok {
		id = ns.(string) + "/" + id
	}
	return types.APIObject{
		Type:   "apple",
		ID:     id,
		Object: &a.Unstructured,
	}
}

func (a apple) with(data map[string]string) apple {
	for k, v := range data {
		a.Object["data"].(map[string]interface{})[k] = v
	}
	return a
}

func (a apple) withNamespace(namespace string) apple {
	a.Object["metadata"].(map[string]interface{})["namespace"] = namespace
	return a
}

type mockAccessSetLookup struct {
	accessID  string
	userRoles []map[string]string
}

func (m *mockAccessSetLookup) AccessFor(user user.Info) *accesscontrol.AccessSet {
	userName := user.GetName()
	access := getAccessID(userName, m.userRoles[0][userName])
	m.userRoles = m.userRoles[1:]
	return &accesscontrol.AccessSet{
		ID: access,
	}
}

func (m *mockAccessSetLookup) PurgeUserData(_ string) {
	panic("not implemented")
}

func getAccessID(user, role string) string {
	h := sha256.Sum256([]byte(user + role))
	return string(h[:])
}

var namespaces = map[string]*corev1.Namespace{
	"n1": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				"field.cattle.io/projectId": "p-abcde",
			},
		},
	},
	"n2": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n2",
			Labels: map[string]string{
				"field.cattle.io/projectId": "p-fghij",
			},
		},
	},
	"n3": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n3",
			Labels: map[string]string{
				"field.cattle.io/projectId": "p-klmno",
			},
		},
	},
	"n4": &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n4",
		},
	},
}

type mockNamespaceCache struct{}

func (m mockNamespaceCache) Get(name string) (*corev1.Namespace, error) {
	return namespaces[name], nil
}

func (m mockNamespaceCache) List(selector labels.Selector) ([]*corev1.Namespace, error) {
	panic("not implemented")
}
func (m mockNamespaceCache) AddIndexer(indexName string, indexer generic.Indexer[*corev1.Namespace]) {
	panic("not implemented")
}
func (m mockNamespaceCache) GetByIndex(indexName, key string) ([]*corev1.Namespace, error) {
	panic("not implemented")
}
