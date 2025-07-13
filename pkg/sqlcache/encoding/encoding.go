package encoding

import (
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func init() {
	gob.Register(map[string]any{})
	gob.Register([]any{})
}

type Encoding string

const (
	EncodingGob      Encoding = "gob"
	EncodingJSON     Encoding = "json"
	EncodingGzipJSON Encoding = "json+gzip"
)

type Decoder struct {
	enc Encoding
	r   io.Reader
}

func NewDecoder(enc Encoding, r io.Reader) *Decoder {
	return &Decoder{
		enc: enc,
		r:   r,
	}
}

func (e *Decoder) Decode(v any) error {
	switch e.enc {
	case EncodingGob:
		dec := gob.NewDecoder(e.r)
		if err := dec.Decode(v); err != nil {
			return fmt.Errorf("decoding %s: %w", e.enc, err)
		}
	case EncodingJSON:
		dec := json.NewDecoder(e.r)
		if err := dec.Decode(v); err != nil {
			return fmt.Errorf("decoding %s: %w", e.enc, err)
		}
	case EncodingGzipJSON:
		r, err := gzip.NewReader(e.r)
		if err != nil {
			return fmt.Errorf("gzip newreader: %w", err)
		}
		defer r.Close()

		dec := json.NewDecoder(r)
		if err := dec.Decode(v); err != nil {
			return fmt.Errorf("decoding %s: %w", e.enc, err)
		}
	default:
		return fmt.Errorf("invalid decoding %s", e.enc)
	}
	return nil
}

type Encoder struct {
	enc Encoding
	w   io.Writer
}

func NewEncoder(enc Encoding, w io.Writer) *Encoder {
	return &Encoder{
		enc: enc,
		w:   w,
	}
}

func (e *Encoder) Encode(obj any) error {
	switch e.enc {
	case EncodingGob:
		enc := gob.NewEncoder(e.w)
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("encoding %s: %w", e.enc, err)
		}
	case EncodingJSON:
		enc := json.NewEncoder(e.w)
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("encoding %s: %w", e.enc, err)
		}
	case EncodingGzipJSON:
		w := gzip.NewWriter(e.w)
		defer w.Close()

		enc := json.NewEncoder(w)
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("encoding %s: %w", e.enc, err)
		}
	default:
		return fmt.Errorf("invalid encoding %s", e.enc)
	}
	return nil
}

func EncodingFromEnv() Encoding {
	encoding := os.Getenv("CATTLE_SQL_CACHE_ENCODING")
	switch encoding {
	case string(EncodingGob):
		return EncodingGob
	case string(EncodingJSON):
		return EncodingJSON
	case string(EncodingGzipJSON):
		return EncodingGzipJSON
	default:
		return EncodingGob
	}
}
