package cache_file

import (
	"github.com/infrago/cache"
)

func Driver() cache.Driver {
	return &fileDriver{}
}

func init() {
	cache.Register("file", Driver())
}
