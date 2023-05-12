steve
=====

Steve is a lightweight API proxy for Kubernetes whose aim is to create an
interface layer suitable for dashboards to efficiently interact with
Kubernetes.

API Usage
---------

### Kubernetes proxy

Requests made to `/api`, `/api/*`, `/apis/*`, `/openapi/*` and `/version` will
be proxied directly to Kubernetes.

### /v1 API

Steve registers all Kubernetes resources as schemas in the /v1 API. Any
endpoint can support methods GET, POST, PATCH, PUT, or DELETE, depending on
what the underlying Kubernetes endpoint supports and the user's permissions.

* `/v1/{type}` - all cluster-scoped resources OR all resources in all
  namespaces of type `{type}` that the user has access to
* `/v1/{type}/{name}` - cluster-scoped resource of type `{type}` and unique name `{name}`
* `/v1/{type}/{namespace}` - all resources of type `{type}` under namespace `{namespace}`
* `/v1/{type}/{namespace}/{name}` - resource of type `{type}` under namespace
  `{namespace}` with name `{name}` unique within the namespace

### Query parameters

Steve supports query parameters to perform actions or process data on top of
what Kubernetes supports.

#### `link`

Trigger a link handler, which is registered with the schema. Examples are
calling the shell for a cluster, or following logs during cluster or catalog
operations:

```
GET /v1/management.cattle.io.clusters/local?link=log
```

#### `action`

Trigger an action handler, which is registered with the schema. Examples are
generating a kubeconfig for a cluster, or installing an app from a catalog:

```
POST /v1/catalog.cattle.io.clusterrepos/rancher-partner-charts?action=install
```

#### `limit`

Only applicable to list requests (`/v1/{type}` and `/v1/{type}/{namespace}`).

Set the maximum number of results to retrieve from Kubernetes. The limit is
passed on as a parameter to the Kubernetes request. The purpose of setting this
limit is to prevent a huge response from overwhelming Steve and Rancher. For
more information about setting limits, review the Kubernetes documentation on
[retrieving results in
chunks](https://kubernetes.io/docs/reference/using-api/api-concepts/#retrieving-large-results-sets-in-chunks).

The limit controls the size of the set coming from Kubernetes, and then
filtering, sorting, and pagination are applied on that set. Because of this, if
the result set is partial, there is no guarantee that the result returned to
the client is fully sorted across the entire list, only across the returned
chunk.

The returned response will include a `continue` token, which indicates that the
result is partial and must be used in the subsequent request to retrieve the
next chunk.

The default limit is 100000. To override the default, set `limit=-1`.

#### `continue`

Only applicable to list requests (`/v1/{type}` and `/v1/{type}/{namespace}`).

Continue retrieving the next chunk of a partial list. The continue token is
included in the response of a limited list and indicates that the result is
partial. This token can then be used as a query parameter to retrieve the next
chunk. All chunks have been retrieved when the continue field in the response
is empty.

#### `filter`

Only applicable to list requests (`/v1/{type}` and `/v1/{type}/{namespace}`).

Filter results by a designated field. Filter keys use dot notation to denote
the subfield of an object to filter on. The filter value is matched as a
substring.

Example, filtering by object name:

```
/v1/{type}?filter=metadata.name=foo
```

One filter can list multiple possible fields to match, these are ORed together:

```
/v1/{type}?filter=metadata.name=foo,metadata.namespace=foo
```

Stacked filters are ANDed together, so an object must match all filters to be
included in the list.

```
/v1/{type}?filter=metadata.name=foo&filter=metadata.namespace=bar
```

Arrays are searched for matching items. If any item in the array matches, the
item is included in the list.

```
/v1/{type}?filter=spec.containers.image=alpine
```

#### `projectsornamespaces`

Resources can also be filtered by the Rancher projects their namespaces belong
to. Since a project isn't an intrinsic part of the resource itself, the filter
parameter for filtering by projects is separate from the main `filter`
parameter. This query parameter is only applicable when steve is runnning in
concert with Rancher.

The list can be filtered by either projects or namespaces or both.

Filtering by a single project or a single namespace:

```
/v1/{type}?projectsornamespaces=p1
```

Filtering by multiple projects or namespaces is done with a comma separated
list. A resource matching any project or namespace in the list is included in
the result:

```
/v1/{type}?projectsornamespaces=p1,n1,n2
```

The list can be negated to exclude results:

```
/v1/{type}?projectsornamespaces!=p1,n1,n2
```

#### `sort`

Only applicable to list requests (`/v1/{type}` and `/v1/{type}/{namespace}`).

Results can be sorted lexicographically by primary and secondary columns.

Sorting by only a primary column, for example name:

```
/v1/{type}?sort=metadata.name
```

Reverse sorting by name:

```
/v1/{type}?sort=-metadata.name
```

The secondary sort criteria is comma separated.

Example, sorting by name and creation time in ascending order:

```
/v1/{type}?sort=metadata.name,metadata.creationTimestamp
```

Reverse sort by name, normal sort by creation time:

```
/v1/{type}?sort=-metadata.name,metadata.creationTimestamp
```

Normal sort by name, reverse sort by creation time:

```
/v1/{type}?sort=metadata.name,-metadata.creationTimestamp
```

#### `page`, `pagesize`, and `revision`

Only applicable to list requests (`/v1/{type}` and `/v1/{type}/{namespace}`).

Results can be batched by pages for easier display.

Example initial request returning a page with 10 results:

```
/v1/{type}?pagesize=10
```

Pages are one-indexed, so this is equivalent to

```
/v1/{type}?pagesize=10&page=1
```
To retrieve subsequent pages, the page number and the list revision number must
be included in the request. This ensures the page will be retrieved from the
cache, rather than making a new request to Kubernetes. If the revision number
is omitted, a new fetch is performed in order to get the latest revision. The
revision is included in the list response.

```
/v1/{type}?pagezie=10&page=2&revision=107440
```

The total number of pages and individual items are included in the list
response as `pages` and `count` respectively.

If a page number is out of bounds, an empty list is returned.

`page` and `pagesize` can be used alongside the `limit` and `continue`
parameters supported by Kubernetes. `limit` and `continue` are typically used
for server-side chunking and do not guarantee results in any order.
