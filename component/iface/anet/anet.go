package anet

import (
	"os"
	"strconv"
)

func IsForceAnet() bool {
	force, _ := strconv.ParseBool(os.Getenv("FORCE_ANET"))
	return force
}
