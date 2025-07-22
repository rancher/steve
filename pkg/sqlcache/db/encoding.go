package db

import (
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"io"
	"strings"
	"sync"
)

type Encoding int

func init() {
	// necessary in order to gob/ungob unstructured.Unstructured objects
	gob.Register(map[string]any{})
	gob.Register([]any{})
}

type encoding interface {
	// Encode serializes an object into the provided writer
	Encode(io.Writer, any) error
	// Decode reads from a provided reader and deserializes into an object
	Decode(io.Reader, any) error
}

type gobEncoding struct{}

func (g gobEncoding) Encode(w io.Writer, obj any) error {
	if err := gob.NewEncoder(w).Encode(obj); err != nil {
		return err
	}
	return nil
}

func (g gobEncoding) Decode(r io.Reader, into any) error {
	return gob.NewDecoder(r).Decode(into)
}

type jsonEncoding struct {
	indentLevel int
}

func (j jsonEncoding) Encode(w io.Writer, obj any) error {
	enc := json.NewEncoder(w)
	if j.indentLevel > 0 {
		enc.SetIndent("", strings.Repeat(" ", j.indentLevel))
	}

	if err := enc.Encode(obj); err != nil {
		return err
	}
	return nil
}

func (j jsonEncoding) Decode(r io.Reader, into any) error {
	return json.NewDecoder(r).Decode(into)
}

type gzipEncoding struct {
	encoding
	writers sync.Pool
	readers sync.Pool
}

func gzipped(wrapped encoding) *gzipEncoding {
	gz := gzipEncoding{
		encoding: wrapped,
	}
	return &gz
}

func (gz *gzipEncoding) Encode(w io.Writer, obj any) error {
	gzw, ok := gz.writers.Get().(*gzip.Writer)
	if !ok {
		gzw = gzip.NewWriter(io.Discard)
	}
	gzw.Reset(w)
	defer func() {
		gzw.Reset(nil)
		gz.writers.Put(gzw)
	}()

	if err := gz.encoding.Encode(gzw, obj); err != nil {
		return err
	}
	return gzw.Close()
}

func (gz *gzipEncoding) Decode(r io.Reader, into any) error {
	gzr, ok := gz.readers.Get().(*gzip.Reader)
	if ok {
		if err := gzr.Reset(r); err != nil {
			return err
		}
	} else {
		var err error
		gzr, err = gzip.NewReader(r)
		if err != nil {
			return err
		}
	}
	defer func() {
		gzr.Close()
		gz.readers.Put(gzr)
	}()

	if err := gz.encoding.Decode(gzr, into); err != nil {
		return err
	}
	return gzr.Close()
}
