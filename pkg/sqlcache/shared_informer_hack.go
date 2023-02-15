package sqlcache

import (
	"reflect"
	"unsafe"
)

func UnsafeSet(object any, field string, value any) {
	rs := reflect.ValueOf(object).Elem()
	rf := rs.FieldByName(field)
	wrf := reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	wrf.Set(reflect.ValueOf(value))
}

func UnsafeGet(object any, field string) any {
	rs := reflect.ValueOf(object).Elem()
	rf := rs.FieldByName(field)
	wrf := reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	return wrf.Interface()
}
