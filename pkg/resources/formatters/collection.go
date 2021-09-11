package formatters

import (
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/norman/types/convert"
)

const (
	maxNumBytesInDataParam = "truncateBytes"
	truncatedField         = "isTruncated"
)

func TruncateBytesInData(request *types.APIRequest, collection *types.GenericCollection) {
	maxNumBytesInDataValue := request.Query.Get(maxNumBytesInDataParam)
	if len(maxNumBytesInDataValue) == 0 {
		return
	}
	maxNumBytes, err := convert.ToNumber(maxNumBytesInDataValue)
	if err != nil {
		return
	}
	for i := 0; i < len(collection.Data); i++ {
		data := collection.Data[i].APIObject.Data()
		resourceDataInt, ok := data["data"]
		if !ok {
			continue
		}
		resourceData := resourceDataInt.(map[string]interface{})
		for k, v := range resourceData {
			if len(v.(string)) > int(maxNumBytes) {
				data[truncatedField] = true
				resourceData[k] = v.(string)[:maxNumBytes]
			}
		}
	}
}
