package sqltypes

type Op string

const (
	Eq        Op = "="
	NotEq     Op = "!="
	Exists    Op = "Exists"
	NotExists Op = "NotExists"
	In        Op = "In"
	NotIn     Op = "NotIn"
	Lt        Op = "Lt"
	Gt        Op = "Gt"
)

// SortOrder represents whether the list should be ascending or descending.
type SortOrder int

const (
	// ASC stands for ascending order.
	ASC SortOrder = iota
	// DESC stands for descending (reverse) order.
	DESC
)

// ListOptions represents the query parameters that may be included in a list request.
type ListOptions struct {
	ChunkSize  int        `json:"chunkSize"`
	Resume     string     `json:"resume"`
	Filters    []OrFilter `json:"orFilters"`
	SortList   SortList   `json:"sortList"`
	Pagination Pagination `json:"pagination"`
}

// Filter represents a field to filter by.
// A subfield in an object is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
// Complex subfields need to be expressed with square brackets, as in `metadata.labels[example.com/moose]`,
// but are mapped to the string slice ["metadata", "labels", "example.com/moose"]
//
// If more than one value is given for the `Match` field, we do an "IN (<values>)" test
type Filter struct {
	Field          []string `json:"fields"`
	Matches        []string `json:"matches"`
	Op             Op       `json:"op"`
	Partial        bool     `json:"partial"`
	IsIndirect     bool     `json:"isIndirect"`
	IndirectFields []string `json:"indirectFields"`
}

// OrFilter represents a set of possible fields to filter by, where an item may match any filter in the set to be included in the result.
type OrFilter struct {
	Filters []Filter
}

// Sort represents the criteria to sort on.
// The subfield to sort by is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
// The order is represented by prefixing the sort key by '-', e.g. sort=-metadata.name.
// e.g. To sort internal clusters first followed by clusters in alpha order: sort=-spec.internal,spec.displayName
type Sort struct {
	Fields         []string  `json:"fields"`
	Order          SortOrder `json:"order"`
	IsIndirect     bool      `json:"isIndirect"`
	IndirectFields []string  `json:"indirectFields"`
}

type SortList struct {
	SortDirectives []Sort `json:"sortDirectives"`
}

// Pagination represents how to return paginated results.
type Pagination struct {
	PageSize int `json:"pageSize"`
	Page     int `json:"page"`
}

func NewSortList() *SortList {
	return &SortList{
		SortDirectives: []Sort{},
	}
}
