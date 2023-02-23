package partition

import (
	"context"
	"github.com/rancher/apiserver/pkg/types"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Partition represents a named grouping of kubernetes resources,
// such as by namespace or a set of names.
type Partition interface {
	Name() string
}

// ParallelPartitionLister defines how a set of partitions will be queried.
type ParallelPartitionLister struct {
	// Lister is the lister method for a single partition.
	Lister PartitionLister

	// Concurrency is the weight of the semaphore.
	Concurrency int64

	// Partitions is the set of partitions that will be concurrently queried.
	Partitions []Partition

	revision string
	err      error
}

// PartitionLister lists objects for one partition.
type PartitionLister func(ctx context.Context, partition Partition, revision string) (*unstructured.UnstructuredList, []types.Warning, error)

// Err returns the latest error encountered.
func (p *ParallelPartitionLister) Err() error {
	return p.err
}

// Revision returns the revision for the current list state.
func (p *ParallelPartitionLister) Revision() string {
	return p.revision
}

// List returns a stream of objects.
// If the continue token is not empty, it decodes it and returns the stream
// starting at the indicated marker.
func (p *ParallelPartitionLister) List(ctx context.Context, revision string) (<-chan []unstructured.Unstructured, error) {
	result := make(chan []unstructured.Unstructured)
	go p.feeder(ctx, revision, result)
	return result, nil
}

// feeder spawns a goroutine to list resources in each partition and feeds the
// results, in order by partition index, into a channel.
func (p *ParallelPartitionLister) feeder(ctx context.Context, revision string, result chan []unstructured.Unstructured) {
	var (
		sem  = semaphore.NewWeighted(p.Concurrency)
		last chan struct{}
	)

	eg, ctx := errgroup.WithContext(ctx)
	defer func() {
		err := eg.Wait()
		if p.err == nil {
			p.err = err
		}
		close(result)
	}()

	for i := 0; i < len(p.Partitions); i++ {
		if isDone(ctx) {
			break
		}

		var (
			partition = p.Partitions[i]
			tickets   = int64(1)
			turn      = last
			next      = make(chan struct{})
		)

		// setup a linked list of channel to control insertion order
		last = next

		if revision == "" {
			// don't have a revision yet so grab all tickets to set a revision
			tickets = 3
		}
		if err := sem.Acquire(ctx, tickets); err != nil {
			p.err = err
			break
		}

		// make revision local for this partition
		revision := revision
		eg.Go(func() error {
			defer sem.Release(tickets)
			defer close(next)

			for {
				list, _, err := p.Lister(ctx, partition, revision)
				if err != nil {
					return err
				}

				waitForTurn(ctx, turn)

				if p.revision == "" {
					p.revision = list.GetResourceVersion()
				}

				result <- list.Items

				return nil
			}
		})
	}

	p.err = eg.Wait()
}

func waitForTurn(ctx context.Context, turn chan struct{}) {
	if turn == nil {
		return
	}
	select {
	case <-turn:
	case <-ctx.Done():
	}
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
