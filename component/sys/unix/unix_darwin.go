package unix

import "golang.org/x/sys/unix"

func SO_NWRITE() uint {
	return unix.SO_NWRITE
}
