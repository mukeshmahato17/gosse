// A generated module for Gosseci functions
//
// This module has been generated via dagger init and serves as a reference to
// basic module structure as you get started with Dagger.
//
// Two functions have been pre-created. You can modify, delete, or add to them,
// as needed. They demonstrate usage of arguments and return types using simple
// echo and grep commands. The functions can be called from the dagger CLI or
// from one of the SDKs.
//
// The first line in this comment block is a short description line and the
// rest is a long description with more detail on the module's purpose or usage,
// if appropriate. All modules should have a short description.

package main

import (
	"context"
	"dagger/gosseci/internal/dagger"
)

type Gosseci struct{}

func (m *Gosseci) RunCI(ctx context.Context, source *dagger.Directory) (string, error) {
	client := dag.Container().
		From("golang:latest").
		WithDirectory("/src", source).
		WithWorkdir("/src")

	goModCache := dag.CacheVolume("go-mod-deps")
	client = client.WithMountedCache("go/pkg/mod", goModCache)

	client = client.WithExec([]string{"make", "deps"})

	return client.WithExec([]string{"make", "test"}).Stdout(ctx)
}
