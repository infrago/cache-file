# cache-file

`cache-file` 是 `cache` 模块的 `file` 驱动。

## 安装

```bash
go get github.com/infrago/cache@latest
go get github.com/infrago/cache-file@latest
```

## 接入

```go
import (
    _ "github.com/infrago/cache"
    _ "github.com/infrago/cache-file"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[cache]
driver = "file"
```

## 公开 API（摘自源码）

- `func (d *fileDriver) Connect(inst *cache.Instance) (cache.Connect, error)`
- `func (c *fileConnection) Open() error`
- `func (c *fileConnection) Close() error`
- `func (c *fileConnection) Read(key string) ([]byte, error)`
- `func (c *fileConnection) Write(key string, val []byte, expire time.Duration) error`
- `func (c *fileConnection) Exists(key string) (bool, error)`
- `func (c *fileConnection) Delete(key string) error`
- `func (c *fileConnection) Sequence(key string, start, step int64, expire time.Duration) (int64, error)`
- `func (c *fileConnection) Keys(prefix string) ([]string, error)`
- `func (c *fileConnection) Clear(prefix string) error`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
