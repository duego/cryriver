// Package bridge is the middleman between different datastores providing helpers that knows about each.
package bridge

// TODO: Migrate mappings from the javascript stuff.
type Transformer interface {
	Transform(*interface{}) *interface{}
}

type NopTransformation struct{}

func (n *NopTransformation) Transform(op *interface{}) *interface{} {
	return op
}
