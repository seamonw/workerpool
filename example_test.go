package workerpool_test

import (
	"context"
	"fmt"

	"github.com/seamonw/workerpool"
)

func Example() {
	p := workerpool.New(4, 16)
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		i := i
		_ = p.Submit(ctx, func() {
			fmt.Print(i)
		})
	}
	p.Close()
	p.Wait()
}
