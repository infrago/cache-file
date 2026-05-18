package cache_file

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/infrago/cache"
	"github.com/infrago/infra"
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
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

func (c *fileConnection) Read(key string) ([]byte, error) {
	if c.db == nil {
		return nil, errors.New("cache db not open")
	}
	var val string
	found := false
	err := c.db.View(func(tx *buntdb.Tx) error {
		v, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return nil
			}
			return err
		}
		val = v
		found = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
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
	vals, err := c.SequenceMany(key, start, step, 1, expire)
	if err != nil {
		return -1, err
	}
	return vals[0], nil
}

func (c *fileConnection) SequenceMany(key string, start, step, count int64, expire time.Duration) ([]int64, error) {
	if c.db == nil {
		return nil, errors.New("cache db not open")
	}
	if count <= 0 {
		return []int64{}, nil
	}
	var current int64
	vals := make([]int64, 0, count)
	err := c.db.Update(func(tx *buntdb.Tx) error {
		val, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		if err == buntdb.ErrNotFound {
			current = start
		} else {
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return err
			}
			current = n + step
		}
		for i := int64(0); i < count; i++ {
			if i > 0 {
				current += step
			}
			vals = append(vals, current)
		}
		opts := (*buntdb.SetOptions)(nil)
		if expire > 0 {
			opts = &buntdb.SetOptions{Expires: true, TTL: expire}
		}
		_, _, err = tx.Set(key, strconv.FormatInt(current, 10), opts)
		return err
	})
	return vals, err
}

func (c *fileConnection) Keys(prefix string) ([]string, error) {
	if c.db == nil {
		return nil, errors.New("cache db not open")
	}
	keys := make([]string, 0)
	err := c.db.View(func(tx *buntdb.Tx) error {
		return c.scanPrefix(tx, prefix, func(k string) bool {
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
			_ = c.scanPrefix(tx, "", func(k string) bool {
				keys = append(keys, k)
				return true
			})
			for _, k := range keys {
				_, _ = tx.Delete(k)
			}
			return nil
		})
	}
	return c.db.Update(func(tx *buntdb.Tx) error {
		keys := make([]string, 0)
		_ = c.scanPrefix(tx, prefix, func(k string) bool {
			keys = append(keys, k)
			return true
		})
		for _, k := range keys {
			_, _ = tx.Delete(k)
		}
		return nil
	})
}

func (c *fileConnection) scanPrefix(tx *buntdb.Tx, prefix string, iter func(key string) bool) error {
	if prefix == "" {
		return tx.Ascend("", func(k, _ string) bool {
			return iter(k)
		})
	}
	return tx.AscendGreaterOrEqual("", prefix, func(k, _ string) bool {
		if !strings.HasPrefix(k, prefix) {
			return false
		}
		return iter(k)
	})
}
