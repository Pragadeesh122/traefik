package server

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/provider/aggregator"
	"github.com/traefik/traefik/v3/pkg/safe"
	th "github.com/traefik/traefik/v3/pkg/testhelpers"
	"github.com/traefik/traefik/v3/pkg/tls"
)

type mockProvider struct {
	messages         []dynamic.Message
	wait             time.Duration
	first            chan struct{}
	throttleDuration time.Duration
}

func (p *mockProvider) Provide(configurationChan chan<- dynamic.Message, _ *safe.Pool) error {
	wait := p.wait
	if wait == 0 {
		wait = 20 * time.Millisecond
	}

	if len(p.messages) == 0 {
		return errors.New("no messages available")
	}

	configurationChan <- p.messages[0]

	if p.first != nil {
		<-p.first
	}

	for _, message := range p.messages[1:] {
		time.Sleep(wait)
		configurationChan <- message
	}

	return nil
}

// ThrottleDuration returns the throttle duration.
func (p *mockProvider) ThrottleDuration() time.Duration {
	return p.throttleDuration
}

func (p *mockProvider) Init() error {
	return nil
}

func TestNewConfigurationWatcher(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())
	t.Cleanup(routinesPool.Stop)

	pvd := &mockProvider{
		messages: []dynamic.Message{{
			ProviderName: "mock",
			Configuration: &dynamic.Configuration{
				HTTP: th.BuildConfiguration(
					th.WithRouters(
						th.WithRouter("test",
							th.WithEntryPoints("e"),
							th.WithServiceName("scv"))),
				),
			},
		}},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	run := make(chan struct{})

	watcher.AddListener(func(conf dynamic.Configuration) {
		expected := dynamic.Configuration{
			HTTP: th.BuildConfiguration(
				th.WithRouters(
					th.WithRouter("test@mock",
						th.WithEntryPoints("e"),
						th.WithServiceName("scv"),
						th.WithObservability())),
				th.WithMiddlewares(),
				th.WithServices(),
			),
			TCP: &dynamic.TCPConfiguration{
				Routers:           map[string]*dynamic.TCPRouter{},
				Middlewares:       map[string]*dynamic.TCPMiddleware{},
				Services:          map[string]*dynamic.TCPService{},
				Models:            map[string]*dynamic.TCPModel{},
				ServersTransports: map[string]*dynamic.TCPServersTransport{},
			},
			TLS: &dynamic.TLSConfiguration{
				Options: map[string]tls.Options{
					"default": tls.DefaultTLSOptions,
				},
				Stores: map[string]tls.Store{},
			},
			UDP: &dynamic.UDPConfiguration{
				Routers:  map[string]*dynamic.UDPRouter{},
				Services: map[string]*dynamic.UDPService{},
			},
		}

		assert.Equal(t, expected, conf)
		close(run)
	})

	watcher.Start()
	<-run
}

func TestWaitForRequiredProvider(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	pvdAggregator := &mockProvider{
		wait: 5 * time.Millisecond,
	}

	config := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bar", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	pvdAggregator.messages = append(pvdAggregator.messages, dynamic.Message{
		ProviderName:  "mock",
		Configuration: config,
	})

	pvdAggregator.messages = append(pvdAggregator.messages, dynamic.Message{
		ProviderName:  "required",
		Configuration: config,
	})

	pvdAggregator.messages = append(pvdAggregator.messages, dynamic.Message{
		ProviderName:  "mock2",
		Configuration: config,
	})

	watcher := NewConfigurationWatcher(routinesPool, pvdAggregator, []string{}, "required", false)

	publishedConfigCount := 0
	watcher.AddListener(func(_ dynamic.Configuration) {
		publishedConfigCount++
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// give some time so that the configuration can be processed
	time.Sleep(20 * time.Millisecond)

	// after 20 milliseconds we should have 2 configs published
	assert.Equal(t, 2, publishedConfigCount, "times configs were published")
}

func TestIgnoreTransientConfiguration(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	config := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bar", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	expectedConfig := dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo@mock", th.WithEntryPoints("ep"), th.WithObservability())),
			th.WithServices(
				th.WithService("bar@mock", th.WithServiceServersLoadBalancer()),
			),
			th.WithMiddlewares(),
		),
		TCP: &dynamic.TCPConfiguration{
			Routers:           map[string]*dynamic.TCPRouter{},
			Middlewares:       map[string]*dynamic.TCPMiddleware{},
			Services:          map[string]*dynamic.TCPService{},
			Models:            map[string]*dynamic.TCPModel{},
			ServersTransports: map[string]*dynamic.TCPServersTransport{},
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  map[string]*dynamic.UDPRouter{},
			Services: map[string]*dynamic.UDPService{},
		},
		TLS: &dynamic.TLSConfiguration{
			Options: map[string]tls.Options{
				"default": tls.DefaultTLSOptions,
			},
			Stores: map[string]tls.Store{},
		},
	}

	expectedConfig3 := dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo@mock", th.WithEntryPoints("ep"), th.WithObservability())),
			th.WithServices(
				th.WithService("bar-config3@mock", th.WithServiceServersLoadBalancer()),
			),
			th.WithMiddlewares(),
		),
		TCP: &dynamic.TCPConfiguration{
			Routers:           map[string]*dynamic.TCPRouter{},
			Middlewares:       map[string]*dynamic.TCPMiddleware{},
			Services:          map[string]*dynamic.TCPService{},
			Models:            map[string]*dynamic.TCPModel{},
			ServersTransports: map[string]*dynamic.TCPServersTransport{},
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  map[string]*dynamic.UDPRouter{},
			Services: map[string]*dynamic.UDPService{},
		},
		TLS: &dynamic.TLSConfiguration{
			Options: map[string]tls.Options{
				"default": tls.DefaultTLSOptions,
			},
			Stores: map[string]tls.Store{},
		},
	}

	config2 := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("baz", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("toto", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	config3 := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bar-config3", th.WithServiceServersLoadBalancer()),
			),
		),
	}
	watcher := NewConfigurationWatcher(routinesPool, &mockProvider{}, []string{}, "", false)

	// To be able to "block" the writes, we change the chan to remove buffering.
	watcher.allProvidersConfigs = make(chan dynamic.Message)

	publishedConfigCount := 0

	firstConfigHandled := make(chan struct{})
	blockConfConsumer := make(chan struct{})
	blockConfConsumerAssert := make(chan struct{})
	watcher.AddListener(func(config dynamic.Configuration) {
		publishedConfigCount++

		if publishedConfigCount > 2 {
			t.Fatal("More than 2 published configuration")
		}

		if publishedConfigCount == 1 {
			assert.Equal(t, expectedConfig, config)
			close(firstConfigHandled)

			<-blockConfConsumer
			time.Sleep(500 * time.Millisecond)
		}

		if publishedConfigCount == 2 {
			assert.Equal(t, expectedConfig3, config)
			close(blockConfConsumerAssert)
		}
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	watcher.allProvidersConfigs <- dynamic.Message{
		ProviderName:  "mock",
		Configuration: config,
	}

	<-firstConfigHandled

	watcher.allProvidersConfigs <- dynamic.Message{
		ProviderName:  "mock",
		Configuration: config2,
	}

	watcher.allProvidersConfigs <- dynamic.Message{
		ProviderName:  "mock",
		Configuration: config,
	}

	close(blockConfConsumer)

	watcher.allProvidersConfigs <- dynamic.Message{
		ProviderName:  "mock",
		Configuration: config3,
	}

	select {
	case <-blockConfConsumerAssert:
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout")
	}
}

func TestListenProvidersThrottleProviderConfigReload(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	pvd := &mockProvider{
		wait:             10 * time.Millisecond,
		throttleDuration: 30 * time.Millisecond,
	}

	for i := range 5 {
		pvd.messages = append(pvd.messages, dynamic.Message{
			ProviderName: "mock",
			Configuration: &dynamic.Configuration{
				HTTP: th.BuildConfiguration(
					th.WithRouters(th.WithRouter("foo"+strconv.Itoa(i), th.WithEntryPoints("ep"))),
					th.WithServices(
						th.WithService("bar", th.WithServiceServersLoadBalancer()),
					),
				),
			},
		})
	}

	providerAggregator := &aggregator.ProviderAggregator{}
	err := providerAggregator.AddProvider(pvd)
	assert.NoError(t, err)

	watcher := NewConfigurationWatcher(routinesPool, providerAggregator, []string{}, "", false)

	publishedConfigCount := 0
	watcher.AddListener(func(_ dynamic.Configuration) {
		publishedConfigCount++
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// Give some time so that the configuration can be processed.
	time.Sleep(100 * time.Millisecond)

	// To load 5 new configs it would require 150ms (5 configs * 30ms).
	// In 100ms, we should only have time to load 3 configs.
	assert.LessOrEqual(t, publishedConfigCount, 3, "config was applied too many times")
	assert.Positive(t, publishedConfigCount, "config was not applied at least once")
}

func TestListenProvidersSkipsEmptyConfigs(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	pvd := &mockProvider{
		messages: []dynamic.Message{{ProviderName: "mock"}},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)
	watcher.AddListener(func(_ dynamic.Configuration) {
		t.Error("An empty configuration was published but it should not")
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// give some time so that the configuration can be processed
	time.Sleep(100 * time.Millisecond)
}

func TestListenProvidersSkipsSameConfigurationForProvider(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	message := dynamic.Message{
		ProviderName: "mock",
		Configuration: &dynamic.Configuration{
			HTTP: th.BuildConfiguration(
				th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
				th.WithServices(
					th.WithService("bar", th.WithServiceServersLoadBalancer()),
				),
			),
		},
	}

	pvd := &mockProvider{
		messages: []dynamic.Message{message, message},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	var configurationReloads int
	watcher.AddListener(func(_ dynamic.Configuration) {
		configurationReloads++
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// give some time so that the configuration can be processed
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, configurationReloads, "Same configuration should not be published multiple times")
}

func TestListenProvidersDoesNotSkipFlappingConfiguration(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	configuration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bar", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	transientConfiguration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("bad", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bad", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	pvd := &mockProvider{
		wait:             5 * time.Millisecond, // The last message needs to be received before the second has been fully processed
		throttleDuration: 15 * time.Millisecond,
		messages: []dynamic.Message{
			{ProviderName: "mock", Configuration: configuration},
			{ProviderName: "mock", Configuration: transientConfiguration},
			{ProviderName: "mock", Configuration: configuration},
		},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	var lastConfig dynamic.Configuration
	watcher.AddListener(func(conf dynamic.Configuration) {
		lastConfig = conf
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// give some time so that the configuration can be processed
	time.Sleep(100 * time.Millisecond)

	expected := dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo@mock", th.WithEntryPoints("ep"), th.WithObservability())),
			th.WithServices(
				th.WithService("bar@mock", th.WithServiceServersLoadBalancer()),
			),
			th.WithMiddlewares(),
		),
		TCP: &dynamic.TCPConfiguration{
			Routers:           map[string]*dynamic.TCPRouter{},
			Middlewares:       map[string]*dynamic.TCPMiddleware{},
			Services:          map[string]*dynamic.TCPService{},
			Models:            map[string]*dynamic.TCPModel{},
			ServersTransports: map[string]*dynamic.TCPServersTransport{},
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  map[string]*dynamic.UDPRouter{},
			Services: map[string]*dynamic.UDPService{},
		},
		TLS: &dynamic.TLSConfiguration{
			Options: map[string]tls.Options{
				"default": tls.DefaultTLSOptions,
			},
			Stores: map[string]tls.Store{},
		},
	}

	assert.Equal(t, expected, lastConfig)
}

func TestListenProvidersIgnoreSameConfig(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	configuration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bar", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	transientConfiguration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("bad", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bad", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	// The transient configuration is sent alternatively with the configuration we want to be applied.
	// It is intended to show that even if the configurations are different,
	// those transient configurations will be ignored if they are sent in a time frame
	// lower than the provider throttle duration.
	pvd := &mockProvider{
		wait:             1 * time.Microsecond, // Enqueue them fast
		throttleDuration: time.Millisecond,
		first:            make(chan struct{}),
		messages: []dynamic.Message{
			{ProviderName: "mock", Configuration: configuration},
			{ProviderName: "mock", Configuration: transientConfiguration},
			{ProviderName: "mock", Configuration: configuration},
			{ProviderName: "mock", Configuration: transientConfiguration},
			{ProviderName: "mock", Configuration: configuration},
		},
	}

	providerAggregator := &aggregator.ProviderAggregator{}
	err := providerAggregator.AddProvider(pvd)
	assert.NoError(t, err)

	watcher := NewConfigurationWatcher(routinesPool, providerAggregator, []string{}, "", false)

	var configurationReloads int
	var lastConfig dynamic.Configuration
	var once sync.Once
	watcher.AddListener(func(conf dynamic.Configuration) {
		configurationReloads++
		lastConfig = conf

		// Allows next configurations to be sent by the mock provider as soon as the first configuration message is applied.
		once.Do(func() {
			pvd.first <- struct{}{}
			// Wait for all configuration messages to pile in
			time.Sleep(5 * time.Millisecond)
		})
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// Wait long enough
	time.Sleep(50 * time.Millisecond)

	expected := dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo@mock", th.WithEntryPoints("ep"), th.WithObservability())),
			th.WithServices(
				th.WithService("bar@mock", th.WithServiceServersLoadBalancer()),
			),
			th.WithMiddlewares(),
		),
		TCP: &dynamic.TCPConfiguration{
			Routers:           map[string]*dynamic.TCPRouter{},
			Middlewares:       map[string]*dynamic.TCPMiddleware{},
			Services:          map[string]*dynamic.TCPService{},
			Models:            map[string]*dynamic.TCPModel{},
			ServersTransports: map[string]*dynamic.TCPServersTransport{},
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  map[string]*dynamic.UDPRouter{},
			Services: map[string]*dynamic.UDPService{},
		},
		TLS: &dynamic.TLSConfiguration{
			Options: map[string]tls.Options{
				"default": tls.DefaultTLSOptions,
			},
			Stores: map[string]tls.Store{},
		},
	}

	assert.Equal(t, expected, lastConfig)

	assert.Equal(t, 1, configurationReloads)
}

func TestApplyConfigUnderStress(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	watcher := NewConfigurationWatcher(routinesPool, &mockProvider{}, []string{}, "", false)

	routinesPool.GoCtx(func(ctx context.Context) {
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case watcher.allProvidersConfigs <- dynamic.Message{ProviderName: "mock", Configuration: &dynamic.Configuration{
				HTTP: th.BuildConfiguration(
					th.WithRouters(th.WithRouter("foo"+strconv.Itoa(i), th.WithEntryPoints("ep"))),
					th.WithServices(
						th.WithService("bar", th.WithServiceServersLoadBalancer()),
					),
				),
			}}:
			}
			i++
		}
	})

	var configurationReloads int
	watcher.AddListener(func(conf dynamic.Configuration) {
		configurationReloads++
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	time.Sleep(100 * time.Millisecond)

	// Ensure that at least two configurations have been applied
	// if we simulate being spammed configuration changes by the provider(s).
	// In theory, checking at least one would be sufficient,
	// but checking for two also ensures that we're looping properly,
	// and that the whole algo holds, etc.
	t.Log(configurationReloads)
	assert.GreaterOrEqual(t, configurationReloads, 2)
}

func TestListenProvidersIgnoreIntermediateConfigs(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	configuration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bar", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	transientConfiguration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("bad", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bad", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	transientConfiguration2 := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("bad2", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bad2", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	finalConfiguration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("final", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("final", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	pvd := &mockProvider{
		wait:             10 * time.Microsecond, // Enqueue them fast
		throttleDuration: 10 * time.Millisecond,
		messages: []dynamic.Message{
			{ProviderName: "mock", Configuration: configuration},
			{ProviderName: "mock", Configuration: transientConfiguration},
			{ProviderName: "mock", Configuration: transientConfiguration2},
			{ProviderName: "mock", Configuration: finalConfiguration},
		},
	}

	providerAggregator := &aggregator.ProviderAggregator{}
	err := providerAggregator.AddProvider(pvd)
	assert.NoError(t, err)

	watcher := NewConfigurationWatcher(routinesPool, providerAggregator, []string{}, "", false)

	var configurationReloads int
	var lastConfig dynamic.Configuration
	watcher.AddListener(func(conf dynamic.Configuration) {
		configurationReloads++
		lastConfig = conf
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// Wait long enough
	time.Sleep(500 * time.Millisecond)

	expected := dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("final@mock", th.WithEntryPoints("ep"), th.WithObservability())),
			th.WithServices(
				th.WithService("final@mock", th.WithServiceServersLoadBalancer()),
			),
			th.WithMiddlewares(),
		),
		TCP: &dynamic.TCPConfiguration{
			Routers:           map[string]*dynamic.TCPRouter{},
			Middlewares:       map[string]*dynamic.TCPMiddleware{},
			Services:          map[string]*dynamic.TCPService{},
			Models:            map[string]*dynamic.TCPModel{},
			ServersTransports: map[string]*dynamic.TCPServersTransport{},
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  map[string]*dynamic.UDPRouter{},
			Services: map[string]*dynamic.UDPService{},
		},
		TLS: &dynamic.TLSConfiguration{
			Options: map[string]tls.Options{
				"default": tls.DefaultTLSOptions,
			},
			Stores: map[string]tls.Store{},
		},
	}

	assert.Equal(t, expected, lastConfig)

	assert.Equal(t, 2, configurationReloads)
}

func TestListenProvidersPublishesConfigForEachProvider(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	configuration := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"))),
			th.WithServices(
				th.WithService("bar", th.WithServiceServersLoadBalancer()),
			),
		),
	}

	pvd := &mockProvider{
		messages: []dynamic.Message{
			{ProviderName: "mock", Configuration: configuration},
			{ProviderName: "mock2", Configuration: configuration},
		},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	var publishedProviderConfig dynamic.Configuration

	watcher.AddListener(func(conf dynamic.Configuration) {
		publishedProviderConfig = conf
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// give some time so that the configuration can be processed
	time.Sleep(100 * time.Millisecond)

	expected := dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(
				th.WithRouter("foo@mock", th.WithEntryPoints("ep"), th.WithObservability()),
				th.WithRouter("foo@mock2", th.WithEntryPoints("ep"), th.WithObservability()),
			),
			th.WithServices(
				th.WithService("bar@mock", th.WithServiceServersLoadBalancer()),
				th.WithService("bar@mock2", th.WithServiceServersLoadBalancer()),
			),
			th.WithMiddlewares(),
		),
		TCP: &dynamic.TCPConfiguration{
			Routers:           map[string]*dynamic.TCPRouter{},
			Middlewares:       map[string]*dynamic.TCPMiddleware{},
			Services:          map[string]*dynamic.TCPService{},
			Models:            map[string]*dynamic.TCPModel{},
			ServersTransports: map[string]*dynamic.TCPServersTransport{},
		},
		TLS: &dynamic.TLSConfiguration{
			Options: map[string]tls.Options{
				"default": tls.DefaultTLSOptions,
			},
			Stores: map[string]tls.Store{},
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  map[string]*dynamic.UDPRouter{},
			Services: map[string]*dynamic.UDPService{},
		},
	}

	assert.Equal(t, expected, publishedProviderConfig)
}

func TestPublishConfigUpdatedByProvider(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	pvdConfiguration := dynamic.Configuration{
		TCP: &dynamic.TCPConfiguration{
			Routers: map[string]*dynamic.TCPRouter{
				"foo": {},
			},
		},
	}

	pvd := &mockProvider{
		wait: 10 * time.Millisecond,
		messages: []dynamic.Message{
			{
				ProviderName:  "mock",
				Configuration: &pvdConfiguration,
			},
			{
				ProviderName:  "mock",
				Configuration: &pvdConfiguration,
			},
		},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	publishedConfigCount := 0
	watcher.AddListener(func(configuration dynamic.Configuration) {
		publishedConfigCount++

		// Update the provider configuration published in next dynamic Message which should trigger a new publishing.
		pvdConfiguration.TCP.Routers["bar"] = &dynamic.TCPRouter{}
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// give some time so that the configuration can be processed.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 2, publishedConfigCount)
}

func TestPublishConfigUpdatedByConfigWatcherListener(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())

	pvd := &mockProvider{
		wait: 10 * time.Millisecond,
		messages: []dynamic.Message{
			{
				ProviderName: "mock",
				Configuration: &dynamic.Configuration{
					TCP: &dynamic.TCPConfiguration{
						Routers: map[string]*dynamic.TCPRouter{
							"foo": {},
						},
					},
				},
			},
			{
				ProviderName: "mock",
				Configuration: &dynamic.Configuration{
					TCP: &dynamic.TCPConfiguration{
						Routers: map[string]*dynamic.TCPRouter{
							"foo": {},
						},
					},
				},
			},
		},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	publishedConfigCount := 0
	watcher.AddListener(func(configuration dynamic.Configuration) {
		publishedConfigCount++

		// Modify the provided configuration.
		// This should not modify the configuration stored in the configuration watcher and therefore there will be no new publishing.
		configuration.TCP.Routers["foo@mock"].Rule = "bar"
	})

	watcher.Start()

	t.Cleanup(watcher.Stop)
	t.Cleanup(routinesPool.Stop)

	// give some time so that the configuration can be processed.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, publishedConfigCount)
}

func TestConfigurationWatcher_MultipleTransformers(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())
	t.Cleanup(routinesPool.Stop)

	pvd := &mockProvider{
		messages: []dynamic.Message{{
			ProviderName: "mock",
			Configuration: &dynamic.Configuration{
				HTTP: th.BuildConfiguration(
					th.WithRouters(
						th.WithRouter("original",
							th.WithEntryPoints("e"),
							th.WithServiceName("scv"))),
				),
			},
		}},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	var callOrder []string

	var callCount1, callCount2 int

	watcher.AddTransformer(func(_ context.Context, configs dynamic.Configurations) dynamic.Configurations {
		callCount1++

		callOrder = append(callOrder, "transformer1")

		for _, config := range configs {
			if config != nil && config.HTTP != nil {
				config.HTTP.Routers["from-transformer1"] = &dynamic.Router{
					EntryPoints: []string{"e"},
					Service:     "svc1",
				}
			}
		}

		return configs
	})

	watcher.AddTransformer(func(_ context.Context, configs dynamic.Configurations) dynamic.Configurations {
		callCount2++

		callOrder = append(callOrder, "transformer2")

		// Verify that transformer1's changes are visible.
		for _, config := range configs {
			if config != nil && config.HTTP != nil {
				assert.Contains(t, config.HTTP.Routers, "from-transformer1")
				config.HTTP.Routers["from-transformer2"] = &dynamic.Router{
					EntryPoints: []string{"e"},
					Service:     "svc2",
				}
			}
		}

		return configs
	})

	run := make(chan struct{})

	watcher.AddListener(func(conf dynamic.Configuration) {
		assert.NotNil(t, conf.HTTP)
		assert.Contains(t, conf.HTTP.Routers, "original@mock")
		assert.Contains(t, conf.HTTP.Routers, "from-transformer1@mock")
		assert.Contains(t, conf.HTTP.Routers, "from-transformer2@mock")
		close(run)
	})

	watcher.Start()
	t.Cleanup(watcher.Stop)

	select {
	case <-run:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for configuration")
	}

	assert.Equal(t, []string{"transformer1", "transformer2"}, callOrder)
	assert.Equal(t, 1, callCount1)
	assert.Equal(t, 1, callCount2)
}

// TestEntryPointTLSResolvedOptions is a regression test for
// https://github.com/traefik/traefik/issues/13289: a router whose TLS
// configuration comes from the entry point (and not from an explicit router TLS
// section) must still have its TLS options resolved in the published configuration.
func TestEntryPointTLSResolvedOptions(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())
	t.Cleanup(routinesPool.Stop)

	pvd := &mockProvider{
		messages: []dynamic.Message{{
			ProviderName: "internal",
			Configuration: &dynamic.Configuration{
				HTTP: &dynamic.HTTPConfiguration{
					Routers: map[string]*dynamic.Router{
						"foo": {
							EntryPoints: []string{"websecure"},
							Rule:        "Host(`foo.example.com`)",
							Service:     "service",
						},
					},
					Models: map[string]*dynamic.Model{
						"websecure": {
							TLS: &dynamic.RouterTLSConfig{},
						},
					},
				},
			},
		}},
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	run := make(chan struct{})
	watcher.AddListener(func(conf dynamic.Configuration) {
		router := conf.HTTP.Routers["foo@internal"]
		if router == nil || router.TLS == nil {
			return
		}

		assert.Equal(t, "default", router.TLS.ResolvedOptions)
		close(run)
	})

	watcher.Start()
	t.Cleanup(watcher.Stop)

	<-run
}

// scriptedProvider provides the given messages, in order, then blocks until the test is
// over.
type scriptedProvider struct {
	messages []dynamic.Message
	interval time.Duration
	block    chan struct{}
}

func (p *scriptedProvider) Provide(configurationChan chan<- dynamic.Message, _ *safe.Pool) error {
	for i, message := range p.messages {
		if i > 0 && p.interval > 0 {
			time.Sleep(p.interval)
		}

		configurationChan <- message
	}

	<-p.block
	return nil
}

func (p *scriptedProvider) Init() error { return nil }

func TestWaitForReferencedProviders(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())
	t.Cleanup(routinesPool.Stop)

	// The configuration of the mock provider refers to the resources of the late
	// provider, which provides them only after the required provider did.
	mockConfig := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("foo",
				th.WithEntryPoints("ep"),
				th.WithRouterMiddlewares("mw@late"),
				th.WithServiceName("svc@late"))),
		),
	}

	requiredConfig := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithRouters(th.WithRouter("required", th.WithEntryPoints("ep"), th.WithServiceName("svc"))),
			th.WithServices(th.WithService("svc", th.WithServiceServersLoadBalancer())),
		),
	}

	lateConfig := &dynamic.Configuration{
		HTTP: th.BuildConfiguration(
			th.WithMiddlewares(th.WithMiddleware("mw", th.WithBasicAuth(&dynamic.BasicAuth{Users: []string{"test"}}))),
			th.WithServices(th.WithService("svc", th.WithServiceServersLoadBalancer())),
		),
	}

	pvd := &scriptedProvider{
		messages: []dynamic.Message{
			{ProviderName: "mock", Configuration: mockConfig},
			{ProviderName: "required", Configuration: requiredConfig},
			{ProviderName: "late", Configuration: lateConfig},
		},
		interval: 30 * time.Millisecond,
		block:    make(chan struct{}),
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "required", false)

	published := make(chan dynamic.Configuration, 10)
	watcher.AddListener(func(conf dynamic.Configuration) { published <- conf })

	watcher.Start()
	t.Cleanup(watcher.Stop)
	t.Cleanup(func() { close(pvd.block) })

	select {
	case conf := <-published:
		// The configuration is applied in one shot, with the resources of the late
		// provider, which the routers refer to.
		assert.Contains(t, conf.HTTP.Routers, "foo@mock")
		assert.Contains(t, conf.HTTP.Routers, "required@required")
		assert.Contains(t, conf.HTTP.Middlewares, "mw@late")
		assert.Contains(t, conf.HTTP.Services, "svc@late")
	case <-time.After(2 * time.Second):
		t.Fatal("the configuration was not applied")
	}

	select {
	case <-published:
		t.Fatal("a partial configuration was applied")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDontWaitForUnreferencedProviders(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())
	t.Cleanup(routinesPool.Stop)

	// The silent provider never provides any configuration, and nothing refers to it:
	// the configuration must not be delayed by it.
	silent := &scriptedProvider{block: make(chan struct{})}

	pvd := &scriptedProvider{
		messages: []dynamic.Message{{
			ProviderName: "required",
			Configuration: &dynamic.Configuration{
				HTTP: th.BuildConfiguration(
					th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"), th.WithServiceName("svc"))),
					th.WithServices(th.WithService("svc", th.WithServiceServersLoadBalancer())),
				),
			},
		}},
		block: make(chan struct{}),
	}

	providerAggregator := &aggregator.ProviderAggregator{}
	require.NoError(t, providerAggregator.AddProvider(silent))
	require.NoError(t, providerAggregator.AddProvider(pvd))

	watcher := NewConfigurationWatcher(routinesPool, providerAggregator, []string{}, "required", false)

	published := make(chan dynamic.Configuration, 10)
	watcher.AddListener(func(conf dynamic.Configuration) { published <- conf })

	watcher.Start()
	t.Cleanup(watcher.Stop)
	t.Cleanup(func() {
		close(pvd.block)
		close(silent.block)
	})

	select {
	case conf := <-published:
		assert.Contains(t, conf.HTTP.Routers, "foo@required")
	case <-time.After(2 * time.Second):
		t.Fatal("the configuration was not applied")
	}
}

func TestApplyConfigurationWhenReferencedProviderHasNothingToProvide(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())
	t.Cleanup(routinesPool.Stop)

	// The referenced provider answers, with nothing to provide: the configuration is
	// applied without waiting for the timeout, and the error of the missing middleware
	// is left to the routers builders.
	pvd := &scriptedProvider{
		messages: []dynamic.Message{
			{ProviderName: "mock", Configuration: &dynamic.Configuration{
				HTTP: th.BuildConfiguration(
					th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"), th.WithRouterMiddlewares("mw@empty"), th.WithServiceName("svc"))),
					th.WithServices(th.WithService("svc", th.WithServiceServersLoadBalancer())),
				),
			}},
			{ProviderName: "empty", Configuration: &dynamic.Configuration{}},
		},
		interval: 20 * time.Millisecond,
		block:    make(chan struct{}),
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)

	published := make(chan dynamic.Configuration, 10)
	watcher.AddListener(func(conf dynamic.Configuration) { published <- conf })

	watcher.Start()
	t.Cleanup(watcher.Stop)
	t.Cleanup(func() { close(pvd.block) })

	select {
	case conf := <-published:
		assert.Contains(t, conf.HTTP.Routers, "foo@mock")
	case <-time.After(2 * time.Second):
		t.Fatal("the configuration was not applied")
	}
}

func TestInitialProvidersTimeout(t *testing.T) {
	routinesPool := safe.NewPool(t.Context())
	t.Cleanup(routinesPool.Stop)

	// The provider which is referred to never provides any configuration.
	pvd := &scriptedProvider{
		messages: []dynamic.Message{{
			ProviderName: "mock",
			Configuration: &dynamic.Configuration{
				HTTP: th.BuildConfiguration(
					th.WithRouters(th.WithRouter("foo", th.WithEntryPoints("ep"), th.WithRouterMiddlewares("mw@silent"))),
					th.WithServices(th.WithService("svc", th.WithServiceServersLoadBalancer())),
				),
			},
		}},
		block: make(chan struct{}),
	}

	watcher := NewConfigurationWatcher(routinesPool, pvd, []string{}, "", false)
	watcher.providersTimeout = 50 * time.Millisecond

	published := make(chan dynamic.Configuration, 10)
	watcher.AddListener(func(conf dynamic.Configuration) { published <- conf })

	watcher.Start()
	t.Cleanup(watcher.Stop)
	t.Cleanup(func() { close(pvd.block) })

	select {
	case conf := <-published:
		assert.Contains(t, conf.HTTP.Routers, "foo@mock")
	case <-time.After(2 * time.Second):
		t.Fatal("the configuration was not applied")
	}
}

func TestReferencedProviders(t *testing.T) {
	configurations := dynamic.Configurations{
		"internal": {
			HTTP: &dynamic.HTTPConfiguration{
				Models: map[string]*dynamic.Model{
					"web": {Middlewares: []string{"entrypoint@file", "internal"}, TLS: &dynamic.RouterTLSConfig{Options: "options@file"}},
				},
				Routers: map[string]*dynamic.Router{
					"dashboard": {Middlewares: []string{"mw@file"}, Service: "svc@file", ParentRefs: []string{"parent@gateway"}},
					"ping":      {Middlewares: []string{"internal"}, Service: "internal"},
				},
				Services: map[string]*dynamic.Service{
					"svc": {Middlewares: []string{"mw@file"}, LoadBalancer: &dynamic.ServersLoadBalancer{ServersTransport: "transport@file"}},
					"weighted": {Weighted: &dynamic.WeightedRoundRobin{
						Services: []dynamic.WRRService{{Name: "one@file"}, {Name: "internal"}},
					}},
					"mirrored": {Mirroring: &dynamic.Mirroring{
						Service: "svc@file",
						Mirrors: []dynamic.MirrorService{{Name: "mirror@file"}},
					}},
					"failed": {Failover: &dynamic.Failover{Service: "svc@file", Fallback: "fallback@file"}},
				},
				Middlewares: map[string]*dynamic.Middleware{
					"mw":     {Chain: &dynamic.Chain{Middlewares: []string{"one@file", "internal"}}},
					"errors": {Errors: &dynamic.ErrorPage{Service: "svc@file"}},
				},
			},
			TCP: &dynamic.TCPConfiguration{
				Routers: map[string]*dynamic.TCPRouter{
					"routed": {Middlewares: []string{"mw@file"}, Service: "svc@file", TLS: &dynamic.RouterTCPTLSConfig{Options: "options@file"}},
				},
				Services: map[string]*dynamic.TCPService{
					"svc":      {LoadBalancer: &dynamic.TCPServersLoadBalancer{ServersTransport: "transport@file"}},
					"weighted": {Weighted: &dynamic.TCPWeightedRoundRobin{Services: []dynamic.TCPWRRService{{Name: "one@file"}}}},
					"local":    {LoadBalancer: &dynamic.TCPServersLoadBalancer{}},
				},
			},
			UDP: &dynamic.UDPConfiguration{
				Routers: map[string]*dynamic.UDPRouter{"routed": {Service: "svc@file"}},
				Services: map[string]*dynamic.UDPService{
					"weighted": {Weighted: &dynamic.UDPWeightedRoundRobin{Services: []dynamic.UDPWRRService{{Name: "one@file"}}}},
				},
			},
		},
		"file": {
			HTTP: &dynamic.HTTPConfiguration{
				Routers: map[string]*dynamic.Router{
					// The references to the provider itself, qualified or not, are not
					// dependencies on another provider.
					"router": {Middlewares: []string{"mw", "mw@file"}, Service: "svc@file"},
				},
			},
		},
	}

	assert.Equal(t, []string{"file", "gateway"}, referencedProviders(configurations))
}
