# coldstore

本地列式数据处理库（纯文件，无数据库）。第一轮实现：批次/列类型/文件段、按列读取与条件筛选。

## 模型

- `Schema`：有序列定义，列类型为 `int64`、`float64`、`bool`、`string`、`timestamp`。
- `Batch`：一批行，按列存放 `ColumnData`，每个非空位置有类型化值，`Null` 位图单独记录空值。
- 存储目录：根目录一个 `manifest.json`；每个批次一个 `batch-N/` 目录，每列一个 `col-i.bin` 段文件。

## 空值与类型规则

- 空值与普通零值分开：null 的 `0`、`0.0`、`""`、`false` 仍为空值。
- 筛选按列定位，绝不使用其他列同位置的标记或值。
- 所有比较遵循三值逻辑：null 对任何比较（含 `!=`）都不命中。
- 比较不做隐式类型转换；筛选字面量类型不符返回错误。

后续加入压缩与更多运算时沿用同样的空值位图与类型规则。

## 只读取需要的列和段

- `ReadOptions.Columns` 只打开被投影的列；`Filters` 引用的列会被读取用于筛选但不返回。
- 清单中记录每段非空数与 min/max，可整批裁剪；裁剪是保守的，null 不计入命中。

## 损坏与发布

- 段格式：`magic(CLS1) | kind | rows | null 位图 | 定长/带长度前缀的值 | CRC32`。
- 段打不开、CRC 不符、载荷非法或与清单不一致时返回 `CorruptSegmentError`，绝不静默跳过。
- 写入时先逐段 fsync 并改名，最后在 `Close` 中原子替换 `manifest.json`；写入中断（未 Close）只留下段文件而没有清单，打开该目录会报错，未完成数据不会发布。

## 参考核对

`refrow` 包提供等价的行式参考实现，测试用它对列式筛选结果逐行核对。

```go
s, _ := coldstore.CreateStore(dir)
w, _ := coldstore.NewWriter(s, mySchema)
_ = w.Append(batch)
_ = w.Close()

r := coldstore.NewReader(mustOpen(t, dir))
results, err := r.Read(coldstore.ReadOptions{
    Columns: []string{"id", "name"},
    Filters: []coldstore.Filter{{Column: "score", Op: coldstore.OpGe, Value: 3.0}},
})
```

运行测试：`go test ./...`。
