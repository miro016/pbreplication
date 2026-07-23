package pbreplication

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
	"github.com/pocketbase/pocketbase/tools/router"
)

// Dedicated replication listener (Config.ReplicationBindAddr).
//
// When configured, the node-to-node endpoints are served on their own
// address — typically a private/intranet interface — instead of the
// public PocketBase port. The router here is deliberately minimal: it
// carries ONLY the node-to-node routes plus the firewall middleware
// (replication-scope rules apply on this listener too); every route is
// protected by the cluster-secret HMAC exactly as on the main port.

// startReplicationListener binds and serves the dedicated listener.
// No-op when ReplicationBindAddr is empty. Binding synchronously means
// a bad address/occupied port fails app startup loudly instead of
// leaving a silently unreachable node.
func (r *Replicator) startReplicationListener() error {
	if r.cfg.ReplicationBindAddr == "" || r.replSrv != nil {
		return nil
	}

	mux := router.NewRouter(func(w http.ResponseWriter, req *http.Request) (*core.RequestEvent, router.EventCleanupFunc) {
		event := new(core.RequestEvent)
		event.Response = w
		event.Request = req
		event.App = r.app
		return event, nil
	})
	mux.Bind(&hook.Handler[*core.RequestEvent]{
		Id:   "pbreplicationFirewall",
		Func: r.firewall.middleware,
	})
	r.registerNodeRoutes(mux.Group("/api/replication"))

	handler, err := mux.BuildMux()
	if err != nil {
		return fmt.Errorf("replication listener router: %w", err)
	}

	ln, err := net.Listen("tcp", r.cfg.ReplicationBindAddr)
	if err != nil {
		return fmt.Errorf("replication listener on %q: %w", r.cfg.ReplicationBindAddr, err)
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 1 * time.Minute,
	}
	r.replLn = ln
	r.replSrv = srv

	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			r.logError("replication listener stopped unexpectedly", err, "addr", ln.Addr().String())
		}
	}()

	r.logInfo("replication listener started", "addr", ln.Addr().String())
	return nil
}

// stopReplicationListener gracefully shuts the dedicated listener down
// (part of the normal shutdown path; safe to call when none is running).
func (r *Replicator) stopReplicationListener() {
	if r.replSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.replSrv.Shutdown(ctx)
	r.replSrv = nil
	r.replLn = nil
}
