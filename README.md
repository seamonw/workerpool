# workerpool

固定大小的 Go 任务池：N 个长期 worker + 有界队列。用来限制**同时在飞的工作数量**，不是用来摊 `go f()` 的创建成本。

配套文章：[goroutine 很轻，所以你更需要任务池](https://seamonw.github.io/blog/2026/09/14/golang-worker-pool/)

```bash
go get github.com/seamonw/workerpool
```

## 快速开始

```go
p := workerpool.New(8, 64) // 8 workers, queue 64
ctx := context.Background()
err := p.Submit(ctx, func() { doWork() })
if err == workerpool.ErrPoolClosed {
    // 已经 Close
}
if err := p.TrySubmit(func() { doWork() }); err == workerpool.ErrPoolFull {
    // 队列满，调用方自己决定丢弃还是 503
}
p.Close()
p.Wait()
```

- `Submit` 会阻塞直到入队、`ctx` 取消或池关闭。
- `TrySubmit` 队列满立刻返回 `ErrPoolFull`。
- 任务 panic 会被 worker 吃掉，池的并发度不会悄悄减一。
- `Close` 与入队共用一把锁，避免往已关闭的 `jobs` 上发送而 panic。

一次请求内的扇出优先用 `errgroup.SetLimit`，不必上进程级池。
