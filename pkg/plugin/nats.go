package plugin

import (
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// connectNats returns the shared NATS connection of the datasource, creating it if there is none yet
// or if the previous one was closed (e.g. because reconnect attempts were exhausted).
func (ds *Datasource) connectNats(options *MyDataSourceOptions, secureOptions *MySecureJsonData) (*nats.Conn, error) {
	ds.natsConnMu.Lock()
	defer ds.natsConnMu.Unlock()

	if ds.natsConn != nil && !ds.natsConn.IsClosed() {
		return ds.natsConn, nil
	}
	if ds.natsConn != nil {
		log.DefaultLogger.Info("NATS connection was closed, reconnecting", "lastErr", ds.natsConn.LastError())
	}

	opts := []nats.Option{
		nats.Name("grafana-nats-datasource"),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.DefaultLogger.Warn("NATS disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.DefaultLogger.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.DefaultLogger.Warn("NATS connection closed", "lastErr", nc.LastError())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			log.DefaultLogger.Error("NATS async error", "subject", subject, "err", err)
		}),
	}

	if options.Authentication == AuthenticationNone {
		// no additional options
	} else if options.Authentication == AuthenticationNkey {
		opts = append(opts, nats.Nkey(
			options.Nkey,
			func(nonce []byte) ([]byte, error) {
				kp, err := nkeys.FromSeed([]byte(secureOptions.NkeySeed))
				if err != nil {
					return nil, fmt.Errorf("unable to load key pair from NkeySeed: %w", err)
				}
				// Wipe our key on exit.
				defer kp.Wipe()

				sig, _ := kp.Sign(nonce)
				return sig, nil
			},
		))
	} else if options.Authentication == AuthenticationUserPass {
		opts = append(opts, nats.UserInfo(options.Username, secureOptions.Password))
	} else if options.Authentication == AuthenticationJWT {
		// Implemented after nats.UserCredentials(), but without temp file
		jwtAsByte := []byte(secureOptions.Jwt)
		userCB := func() (string, error) {
			return nkeys.ParseDecoratedJWT(jwtAsByte)
		}
		sigCB := func(nonce []byte) ([]byte, error) {
			keyPair, err := nkeys.ParseDecoratedNKey(jwtAsByte)
			if err != nil {
				return nil, fmt.Errorf("unable to extract key pair from file: %w", err)
			}
			// Wipe our key on exit.
			defer keyPair.Wipe()

			sig, _ := keyPair.Sign(nonce)
			return sig, nil
		}

		opts = append(opts, nats.UserJWT(userCB, sigCB))
	} else {
		// TODO: TOKEN AUTH
		return nil, fmt.Errorf("TODO")
	}

	nc, err := nats.Connect(options.NatsUrl, opts...)
	if err != nil {
		return nil, err
	}
	ds.natsConn = nc
	return nc, nil
}

// closeNats closes the shared NATS connection, if any.
func (ds *Datasource) closeNats() {
	ds.natsConnMu.Lock()
	defer ds.natsConnMu.Unlock()

	if ds.natsConn != nil {
		ds.natsConn.Close()
		ds.natsConn = nil
	}
}
