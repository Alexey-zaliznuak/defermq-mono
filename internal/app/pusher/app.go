package pusher

import (
	"context"
	"errors"
	"fmt"

	"github.com/defermq/defermq/internal/delivery"
	"golang.org/x/sync/errgroup"
)

type App struct {
	pools      []*Pool
	consumers  []Consumer
	repository Repository
	dispatcher *delivery.Dispatcher
}

func NewApp(
	pools []*Pool,
	consumers []Consumer,
	repository Repository,
	dispatcher *delivery.Dispatcher,
) (*App, error) {
	if len(pools) == 0 || repository == nil || dispatcher == nil {
		return nil, errors.New("Pusher requires at least one worker pool")
	}
	return &App{
		pools:      pools,
		consumers:  consumers,
		repository: repository,
		dispatcher: dispatcher,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)
	for _, pool := range a.pools {
		pool := pool
		group.Go(func() error { return pool.Run(groupCtx) })
	}
	return group.Wait()
}

func (a *App) Ready(ctx context.Context) error {
	if err := a.repository.Ready(ctx); err != nil {
		return fmt.Errorf("source postgres: %w", err)
	}
	for _, consumer := range a.consumers {
		if err := consumer.Ready(ctx); err != nil {
			return fmt.Errorf("%s consumer: %w", consumer.Type(), err)
		}
	}
	return a.dispatcher.Ready(ctx)
}

func (a *App) Close(ctx context.Context) error {
	var combined error
	for _, consumer := range a.consumers {
		combined = errors.Join(combined, consumer.Close(ctx))
	}
	return errors.Join(combined, a.dispatcher.Close(ctx))
}
