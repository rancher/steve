package helmrelease

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/helm/pkg/proto/hapi/release"
)

func ToRelease(data, name string) (*HelmRelease, error) {
	bytes, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}

	var hr release.Release
	if err := proto.Unmarshal(bytes, &hr); err != nil {
		return nil, err
	}

	if hr.Chart == nil || hr.Chart.Metadata == nil {
		return nil, fmt.Errorf("invalid chart, missing chart or metadata")
	}

	hrVersion := HelmRelease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: hr.Namespace,
		},
		ID:            fmt.Sprintf("%s:%s", hr.Namespace, name),
		Name:          hr.Name,
		FirstDeployed: toTime(hr.Info.FirstDeployed),
		LastDeployed:  toTime(hr.Info.LastDeployed),
		Deleted:       toTime(hr.Info.Deleted),
		Metadata:      *hr.Chart.Metadata,
		Status:        release.Status_Code_name[int32(hr.Info.Status.Code)],
		Manifest:      hr.Manifest,
		Version:       hr.Version,
	}

	if hr.Info.Status != nil {
		hrVersion.Status = release.Status_Code_name[int32(hr.Info.Status.Code)]
		for _, template := range hr.Chart.Templates {
			if strings.EqualFold("readme.md", template.Name) {
				hrVersion.ReadMe = string(template.Data)
			}
		}
	}

	return &hrVersion, nil
}

func toTime(t *timestamp.Timestamp) *metav1.Time {
	if t == nil {
		return nil
	}
	time := metav1.NewTime(time.Unix(t.Seconds, int64(t.Nanos)))
	return &time
}
