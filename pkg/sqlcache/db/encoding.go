package db

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
)

type Encoding int

const (
	GobEncoding Encoding = iota
	JSONEncoding
	GzippedGobEncoding
	GzippedJSONEncoding
)

var defaultEncoding encoding = &gobEncoding{}

func init() {
	// necessary in order to gob/ungob unstructured.Unstructured objects
	gob.Register(map[string]any{})
	gob.Register([]any{})

	// Allow using JSON encoding during development
	if enc := os.Getenv("CATTLE_SQL_CACHE_ENCODING"); enc == "json" {
		defaultEncoding = &jsonEncoding{}
	}
}

func encodingForType(encType Encoding) encoding {
	switch encType {
	case GobEncoding:
		return &gobEncoding{}
	case JSONEncoding:
		return jsonEncoding{}
	case GzippedGobEncoding:
		return gzipped(&gobEncoding{})
	case GzippedJSONEncoding:
		return gzipped(jsonEncoding{})
	}
	// unreachable
	return defaultEncoding
}

func WithEncoding(encType Encoding) ClientOption {
	return func(c *client) {
		c.encoding = encodingForType(encType)
	}
}

type encoding interface {
	// Encode serializes an object into the provided writer
	Encode(io.Writer, any) error
	// Decode reads from a provided reader and deserializes into an object
	Decode(io.Reader, any) error
}

type gobEncoding struct {
	writeLock, readLock sync.Mutex
	writeBuf, readBuf   bytes.Buffer
	encoder             *gob.Encoder
	decoder             *gob.Decoder
	seenTypes           map[reflect.Type]struct{}
}

func (g *gobEncoding) Encode(w io.Writer, obj any) error {
	g.writeLock.Lock()
	defer g.writeLock.Unlock()

	if g.encoder == nil {
		g.encoder = gob.NewEncoder(&g.writeBuf)
	}
	g.writeBuf.Reset()

	// gob encoders and decoders share extra types information the first time a certain object type is transferred
	if err := g.registerTypeIfNeeded(obj); err != nil {
		return err
	}

	// Encode to the internal write buffer
	if err := g.encoder.Encode(obj); err != nil {
		return err
	}

	// Finally copy from internal buffer to the destination
	_, err := g.writeBuf.WriteTo(w)
	return err
}

// registerTypeIfNeeded prevents future decoding errors by running Decode right after the first Encode for an object type
// This is needed when reusing a gob.Encoder, as it assumes the receiving end of those messages is always the same gob.Decoder.
// Due to this assumption, it applies some optimizations, like avoiding sending complete type's information (needed for decoding) if it already did it before.
// This means the first object for each type encoded by a gob.Encoder will have a bigger payload, but more importantly,
// the decoding will fail if this is not the first object being decoded later. This function forces that to prevent this from happening.
func (g *gobEncoding) registerTypeIfNeeded(obj any) error {
	if g.seenTypes == nil {
		g.seenTypes = make(map[reflect.Type]struct{})
	}

	typ := reflect.TypeOf(obj)
	if _, ok := g.seenTypes[typ]; ok {
		return nil
	}

	if err := g.encoder.Encode(obj); err != nil {
		return err
	}
	defer g.writeBuf.Reset()

	// Decode into a new object to avoid modifying the original. This let the decoder consume the extra headers sent by the encoder only the first time
	newObj := reflect.New(typ).Interface()
	if err := g.Decode(bytes.NewReader(g.writeBuf.Bytes()), newObj); err != nil {
		return fmt.Errorf("could not decode %T: %w", obj, err)
	}

	g.seenTypes[typ] = struct{}{}
	return nil
}

func (g *gobEncoding) Decode(r io.Reader, into any) error {
	g.readLock.Lock()
	defer g.readLock.Unlock()

	if g.decoder == nil {
		g.decoder = gob.NewDecoder(&g.readBuf)
	}

	g.readBuf.Reset()
	if _, err := g.readBuf.ReadFrom(r); err != nil {
		return err
	}
	return g.decoder.Decode(into)
}

type jsonEncoding struct {
	indentLevel int
}

func (j jsonEncoding) Encode(w io.Writer, obj any) error {
	enc := json.NewEncoder(w)
	if j.indentLevel > 0 {
		enc.SetIndent("", strings.Repeat(" ", j.indentLevel))
	}

	return enc.Encode(obj)
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
