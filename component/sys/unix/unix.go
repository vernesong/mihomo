//go:build !(darwin || linux)

package unix

func SO_NWRITE() uint {
	panic("not implemented")
}
