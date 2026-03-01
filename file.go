package cache_file

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/infrago/infra"
	"github.com/infrago/cache"
	"github.com/tidwall/buntdb"
)

type fileDriver struct{}

type fileConnection struct {
	path string
	db   *buntdb.DB
}

func init() {
	infra.Register("file", &fileDriver{})
}

func (d *fileDriver) Connect(inst *cache.Instance) (cache.Connect, error) {
	path, _ := inst.Config.Setting["file"].(string)
	if path == "" {
		path, _ = inst.Config.Setting["path"].(string)
	}
	if path == "" {
		path, _ = inst.Config.Setting["db"].(string)
	}
	if path == "" {
		path = "cache.db"
	}
	return &fileConnection{path: path}, nil
}

func (c *fileConnection) Open() error {
	if c.db != nil {
		return nil
	}
	if dir := filepath.Dir(c.path); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	db, err := buntdb.Open(c.path)
	if err != nil {
		return err
	}
	c.db = db
	return nil
}

func (c *fileConnection) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *fileConnection) Read(key string) ([]byte, error) {
	if c.db == nil {
		return nil, errors.New("cache db not open")
	}
	var val string
	err := c.db.View(func(tx *buntdb.Tx) error {
		v, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return nil
			}
			return err
		}
		val = v
		return nil
	})
	if err != nil {
		return nil, err
	}
	if val == "" {
		return nil, nil
	}
	return []byte(val), nil
}

func (c *fileConnection) Write(key string, val []byte, expire time.Duration) error {
	if c.db == nil {
		return errors.New("cache db not open")
	}
	return c.db.Update(func(tx *buntdb.Tx) error {
		opts := (*buntdb.SetOptions)(nil)
		if expire > 0 {
			opts = &buntdb.SetOptions{Expires: true, TTL: expire}
		}
		_, _, err := tx.Set(key, string(val), opts)
		return err
	})
}

func (c *fileConnection) Exists(key string) (bool, error) {
	if c.db == nil {
		return false, errors.New("cache db not open")
	}
	found := false
	err := c.db.View(func(tx *buntdb.Tx) error {
		_, err := tx.Get(key)
		if err == nil {
			found = true
			return nil
		}
		if err == buntdb.ErrNotFound {
			return nil
		}
		return err
	})
	return found, err
}

func (c *fileConnection) Delete(key string) error {
	if c.db == nil {
		return errors.New("cache db not open")
	}
	return c.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		if err == buntdb.ErrNotFound {
			return nil
		}
		return err
	})
}

func (c *fileConnection) Sequence(key string, start, step int64, expire time.Duration) (int64, error) {
	if c.db == nil {
		return -1, errors.New("cache db not open")
	}
	var current int64
	err := c.db.Update(func(tx *buntdb.Tx) error {
		val, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		if val == "" {
			current = start
		} else {
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return err
			}
			current = n + step
		}
		opts := (*buntdb.SetOptions)(nil)
		if expire > 0 {
			opts = &buntdb.SetOptions{Expires: true, TTL: expire}
		}
		_, _, err = tx.Set(key, strconv.FormatInt(current, 10), opts)
		return err
	})
	return current, err
}

func (c *fileConnection) Keys(prefix string) ([]string, error) {
	if c.db == nil {
		return nil, errors.New("cache db not open")
	}
	if prefix == "" {
		prefix = "*"
	} else if !strings.HasSuffix(prefix, "*") {
		prefix = prefix + "*"
	}
	keys := make([]string, 0)
	err := c.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(prefix, func(k, _ string) bool {
			keys = append(keys, k)
			return true
		})
	})
	return keys, err
}

func (c *fileConnection) Clear(prefix string) error {
	if c.db == nil {
		return errors.New("cache db not open")
	}
	if prefix == "" {
		return c.db.Update(func(tx *buntdb.Tx) error {
			keys := make([]string, 0)
			_ = tx.AscendKeys("*", func(k, _ string) bool {
				keys = append(keys, k)
				return true
			})
			for _, k := range keys {
				_, _ = tx.Delete(k)
			}
			return nil
		})
	}
	if !strings.HasSuffix(prefix, "*") {
		prefix = prefix + "*"
	}
	return c.db.Update(func(tx *buntdb.Tx) error {
		keys := make([]string, 0)
		_ = tx.AscendKeys(prefix, func(k, _ string) bool {
			keys = append(keys, k)
			return true
		})
		for _, k := range keys {
			_, _ = tx.Delete(k)
		}
		return nil
	})
}
