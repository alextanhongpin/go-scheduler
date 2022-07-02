package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	readTimeout     = 5 * time.Second
	shutdownTimeout = 5 * time.Second
	handlerTimeout  = 5 * time.Second
)

func New(handler http.Handler, port int) {
	log := log.With().Str("pkg", "server").Logger()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		ReadHeaderTimeout: readTimeout,
		ReadTimeout:       readTimeout,
		// Instead of setting WriteTimeout, we use http.TimeoutHandler to specify the maximum amount of time for a handler to complete.
		Handler: http.TimeoutHandler(handler, handlerTimeout, ""),
	}

	// Initializing the srv in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Err(err).Msg("listen error")
		}
	}()

	// Listen for the interrupt signal.
	<-ctx.Done()

	// Restore default behaviour on the interrupt signal and notify user of shutdown.
	stop()

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Err(err).Msg("forced to shut down")
	} else {
		log.Info().Msg("exiting")
	}
}
