package env

import "os"

func defaultEnviron() []string {
	return os.Environ()
}
