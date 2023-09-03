package mr

import (
	"fmt"
	"os"
	"strconv"
)

func dprint(a ...interface{}) {
	if env, _ := strconv.Atoi(os.Getenv("V")); env == 1 {
		fmt.Println(a...)
	}
}
