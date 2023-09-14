package mr

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

var outfile *os.File = nil
var env int = 0

func init() {
	env, _ = strconv.Atoi(os.Getenv("V"))
	if env == 0 {
		outfile, _ = os.Create("dprint.log")
		log.SetOutput(outfile)
	}
}

func dprint(who string, a ...interface{}) {
	str := fmt.Sprintln(a...)
	if env == 0 { // default to file only
		outfile.WriteString(who + ": " + str)
	} else if env == 1 { // to std
		fmt.Print(who + ": " + str)
	} else {
		panic("unknown verbose level")
	}
}
