package tablelistconvert

import (
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	watch2 "k8s.io/apimachinery/pkg/watch"
	"testing"
	"time"
)

//go:generate mockgen --build_flags=--mod=mod -package tablelistconvert -destination ./dynamic_mocks_test.go k8s.io/client-go/dynamic ResourceInterface
//go:generate mockgen --build_flags=--mod=mod -package tablelistconvert -destination ./watch_mocks_test.go k8s.io/apimachinery/pkg/watch Interface

func TestWatch(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "client Watch() with no errors returned should returned no errors. Objects passed to underlying channel should" +
			" be sent with expected metadata.fields",
		test: func(t *testing.T) {
			ri := NewMockResourceInterface(gomock.NewController(t))
			watch := NewMockInterface(gomock.NewController(t))
			testEvents := make(chan watch2.Event)
			opts := metav1.ListOptions{}
			ri.EXPECT().Watch(context.TODO(), opts).Return(watch, nil)
			initialEvent := watch2.Event{
				Object: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Table",
						"apiVersion": "meta.k8s.io/v1",
						"rows": []interface{}{
							map[string]interface{}{
								"cells":  []interface{}{"cell1", "cell2"},
								"object": map[string]interface{}{},
							},
						},
					},
				},
			}
			expectedEvent := watch2.Event{
				Object: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"fields": []interface{}{"cell1", "cell2"},
						},
					},
				},
			}
			go func() {
				time.Sleep(1 * time.Second)
				testEvents <- initialEvent
			}()
			watch.EXPECT().ResultChan().Return(testEvents)
			client := &Client{ResourceInterface: ri}
			receivedWatch, err := client.Watch(context.TODO(), opts)
			assert.Nil(t, err)
			receivedEvent, ok := <-receivedWatch.ResultChan()
			assert.True(t, ok)
			assert.Equal(t, expectedEvent, receivedEvent)
		},
	})
	tests = append(tests, testCase{
		description: "client Watch() with no errors returned should returned no errors. Objects passed to underlying channel that are not of type \"table\"" +
			" should not be sent with metadata.fields",
		test: func(t *testing.T) {
			ri := NewMockResourceInterface(gomock.NewController(t))
			watch := NewMockInterface(gomock.NewController(t))
			testEvents := make(chan watch2.Event)
			opts := metav1.ListOptions{}
			ri.EXPECT().Watch(context.TODO(), opts).Return(watch, nil)
			initialEvent := watch2.Event{
				Object: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "NotTable",
						"apiVersion": "meta.k8s.io/v1",
						"rows": []interface{}{
							map[string]interface{}{
								"cells":  []interface{}{"cell1", "cell2"},
								"object": map[string]interface{}{},
							},
						},
					},
				},
			}
			expectedEvent := watch2.Event{
				Object: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "NotTable",
						"apiVersion": "meta.k8s.io/v1",
						"rows": []interface{}{
							map[string]interface{}{
								"cells":  []interface{}{"cell1", "cell2"},
								"object": map[string]interface{}{},
							},
						},
					},
				},
			}
			go func() {
				time.Sleep(1 * time.Second)
				testEvents <- initialEvent
			}()
			watch.EXPECT().ResultChan().Return(testEvents)
			client := &Client{ResourceInterface: ri}
			receivedWatch, err := client.Watch(context.TODO(), opts)
			assert.Nil(t, err)
			receivedEvent, ok := <-receivedWatch.ResultChan()
			assert.True(t, ok)
			assert.Equal(t, expectedEvent, receivedEvent)
		},
	})
	tests = append(tests, testCase{
		description: "client Watch() with no errors returned should returned no errors. Nil objects passed to underlying" +
			" channel should be sent as nil",
		test: func(t *testing.T) {
			ri := NewMockResourceInterface(gomock.NewController(t))
			watch := NewMockInterface(gomock.NewController(t))
			testEvents := make(chan watch2.Event)
			opts := metav1.ListOptions{}
			ri.EXPECT().Watch(context.TODO(), opts).Return(watch, nil)
			initialEvent := watch2.Event{
				Object: &unstructured.Unstructured{
					nil,
				},
			}
			expectedEvent := watch2.Event{
				Object: &unstructured.Unstructured{
					Object: nil,
				},
			}
			go func() {
				time.Sleep(1 * time.Second)
				testEvents <- initialEvent
			}()
			watch.EXPECT().ResultChan().Return(testEvents)
			client := &Client{ResourceInterface: ri}
			receivedWatch, err := client.Watch(context.TODO(), opts)
			assert.Nil(t, err)
			receivedEvent, ok := <-receivedWatch.ResultChan()
			assert.True(t, ok)
			assert.Equal(t, expectedEvent, receivedEvent)
		},
	})
	tests = append(tests, testCase{
		description: "client Watch() with error returned from resource client should returned an errors",
		test: func(t *testing.T) {
			ri := NewMockResourceInterface(gomock.NewController(t))
			opts := metav1.ListOptions{}
			ri.EXPECT().Watch(context.TODO(), opts).Return(nil, fmt.Errorf("error"))
			client := &Client{ResourceInterface: ri}
			_, err := client.Watch(context.TODO(), opts)
			assert.NotNil(t, err)
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestList(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "client List() with no errors returned should returned no errors. Received list should be mutated" +
			"to contain rows.objects in Objects field and metadata.fields should be added to both.",
		test: func(t *testing.T) {
			ri := NewMockResourceInterface(gomock.NewController(t))
			opts := metav1.ListOptions{}
			initialList := &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind":       "Table",
					"apiVersion": "meta.k8s.io/v1",
					"rows": []interface{}{
						map[string]interface{}{
							"cells":  []interface{}{"cell1", "cell2"},
							"object": map[string]interface{}{},
						},
					},
				},
				Items: []unstructured.Unstructured{
					{},
				},
			}
			expectedList := &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind":       "Table",
					"apiVersion": "meta.k8s.io/v1",
					"rows": []interface{}{
						map[string]interface{}{
							"cells": []interface{}{"cell1", "cell2"},
							"object": map[string]interface{}{
								"metadata": map[string]interface{}{
									"fields": []interface{}{"cell1", "cell2"},
								}},
						},
					},
				},
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"fields": []interface{}{"cell1", "cell2"},
						},
					}},
				},
			}
			ri.EXPECT().List(context.TODO(), opts).Return(initialList, nil)
			client := &Client{ResourceInterface: ri}
			receivedList, err := client.List(context.TODO(), opts)
			assert.Nil(t, err)
			assert.Equal(t, expectedList, receivedList)
		},
	})
	tests = append(tests, testCase{
		description: "client List() with no errors returned should returned no errors. Received list should be not mutated" +
			"if kind is not \"Table\".",
		test: func(t *testing.T) {
			ri := NewMockResourceInterface(gomock.NewController(t))
			opts := metav1.ListOptions{}
			initialList := &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind":       "NotTable",
					"apiVersion": "meta.k8s.io/v1",
					"rows": []interface{}{
						map[string]interface{}{
							"cells":  []interface{}{"cell1", "cell2"},
							"object": map[string]interface{}{},
						},
					},
				},
				Items: []unstructured.Unstructured{
					{},
				},
			}
			ri.EXPECT().List(context.TODO(), opts).Return(initialList, nil)
			client := &Client{ResourceInterface: ri}
			receivedList, err := client.List(context.TODO(), opts)
			assert.Nil(t, err)
			assert.Equal(t, initialList, receivedList)
		},
	})
	tests = append(tests, testCase{
		description: "client List() with errors returned from Resource Interface should returned an error" +
			"if kind is not \"Table\".",
		test: func(t *testing.T) {
			ri := NewMockResourceInterface(gomock.NewController(t))
			opts := metav1.ListOptions{}
			ri.EXPECT().List(context.TODO(), opts).Return(nil, fmt.Errorf("error"))
			client := &Client{ResourceInterface: ri}
			_, err := client.List(context.TODO(), opts)
			assert.NotNil(t, err)
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
