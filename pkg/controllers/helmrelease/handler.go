package helmrelease

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type HelmRelease struct {
}

type handler struct {
	releases map[string]HelmRelease
}

func (h *handler) OnConfigMapChange(key string, obj *v1.ConfigMap) (*v1.ConfigMap, error) {
	if !h.isRelease(obj) {
		return obj, nil
	}

}

func (h *handler) OnSecretChange(key string, obj *v1.Secret) (*v1.Secret, error) {
	if !h.isRelease(obj) {
		return obj, nil
	}

}

func (n *handler) isRelease(obj metav1.Object) bool {
	if obj == nil {
		return false
	}
	return obj.GetLabels()["OWNER"] == "TILLER"
}
