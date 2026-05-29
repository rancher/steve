package sqlgen

import (
	"hash/fnv"
	"io"
	"maps"
	"slices"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"k8s.io/apimachinery/pkg/util/sets"
)

// CompilePartitions compiles partition RBAC constraints into an Expr.
// Returns nil if no partition filtering is needed (passthrough/full access).
func CompilePartitions(namespaceFilter string, partitions []partition.Partition, prefix string) sqlexpr.Expr {
	if len(partitions) == 0 {
		return sqlexpr.Raw{SQL: "FALSE"}
	}

	type group struct {
		names         []string
		namespaces    []string
		allNamespaces bool
	}

	groups := make(map[uint64]*group)
	singleNamespace := namespaceFilter != "" && namespaceFilter != "*"

	for _, thisPartition := range partitions {
		filterByNamespace := thisPartition.Namespace != "" && thisPartition.Namespace != "*"
		filterByNames := !thisPartition.All

		// Passthrough provides access to everything
		if thisPartition.Passthrough || (!filterByNamespace && !filterByNames) {
			return nil
		}

		if singleNamespace && filterByNamespace && thisPartition.Namespace != namespaceFilter {
			continue
		}

		var sig uint64
		var names []string
		if filterByNames {
			names = sets.List(thisPartition.Names)
			if len(names) == 0 {
				continue
			}
			sig = namesSignatures(names)
			if sig == 0 {
				sig = 1
			}
		}

		g, ok := groups[sig]
		if !ok {
			g = &group{names: names}
			groups[sig] = g
		}
		if !filterByNamespace {
			g.allNamespaces = true
			g.namespaces = nil
		}
		if !g.allNamespaces {
			g.namespaces = append(g.namespaces, thisPartition.Namespace)
		}
	}

	if len(groups) == 0 {
		return sqlexpr.Raw{SQL: "FALSE"}
	}

	// Special case: full namespace access with no name restrictions
	if g, ok := groups[0]; ok && (singleNamespace || g.allNamespaces) {
		return nil
	}

	nsCol := sqlexpr.Col{Table: prefix, Name: "metadata.namespace"}
	nameCol := sqlexpr.Col{Table: prefix, Name: "metadata.name"}

	var clauses []sqlexpr.Expr
	for _, sig := range slices.Sorted(maps.Keys(groups)) {
		g := groups[sig]
		slices.Sort(g.namespaces)

		switch {
		case sig == 0:
			// Full namespace access (no name restrictions)
			params := make([]sqlexpr.Expr, len(g.namespaces))
			for i, ns := range g.namespaces {
				params[i] = sqlexpr.Param{Value: ns}
			}
			clauses = append(clauses, sqlexpr.In{Expr: nsCol, Values: params})

		case !singleNamespace && !g.allNamespaces:
			// Restricted names in specific namespaces
			nsParams := make([]sqlexpr.Expr, len(g.namespaces))
			for i, ns := range g.namespaces {
				nsParams[i] = sqlexpr.Param{Value: ns}
			}
			nameParams := make([]sqlexpr.Expr, len(g.names))
			for i, name := range g.names {
				nameParams[i] = sqlexpr.Param{Value: name}
			}
			clauses = append(clauses, sqlexpr.And{
				sqlexpr.In{Expr: nsCol, Values: nsParams},
				sqlexpr.In{Expr: nameCol, Values: nameParams},
			})

		default:
			// Restricted names, all namespaces (or single namespace already filtered)
			nameParams := make([]sqlexpr.Expr, len(g.names))
			for i, name := range g.names {
				nameParams[i] = sqlexpr.Param{Value: name}
			}
			clauses = append(clauses, sqlexpr.In{Expr: nameCol, Values: nameParams})
		}
	}

	if len(clauses) == 1 {
		return clauses[0]
	}
	return sqlexpr.Or(clauses)
}

func namesSignatures(names []string) uint64 {
	h := fnv.New64a()
	for _, name := range names {
		io.WriteString(h, name)
		io.WriteString(h, "\x00")
	}
	return h.Sum64()
}
