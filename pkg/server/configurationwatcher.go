package server

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/observability/logs"
	"github.com/traefik/traefik/v3/pkg/provider"
	"github.com/traefik/traefik/v3/pkg/safe"
	"github.com/traefik/traefik/v3/pkg/tls"
	"github.com/traefik/traefik/v3/pkg/types"
)

// defaultProvidersTimeout is how long the first configuration is held back, waiting for
// the providers it refers to, to send their own first configuration.
//
// Providers are started concurrently, so there is no ordering between their first
// configurations: applying a configuration which refers to a resource of a provider
// which did not speak yet disables every router using that resource, and makes them
// report a spurious "does not exist" error, until that provider provides its own
// configuration.
//
// The timeout is what keeps Traefik usable when a provider is never going to answer,
// i.e. when a resource refers to a provider which is not enabled, or when the backend
// of a provider is unreachable.
const defaultProvidersTimeout = 10 * time.Second

// ConfigurationWatcher watches configuration changes.
type ConfigurationWatcher struct {
	providerAggregator provider.Provider

	defaultEntryPoints []string

	strictTLSOptions bool

	allProvidersConfigs chan dynamic.Message

	newConfigs chan dynamic.Configurations

	requiredProvider       string
	providersTimeout       time.Duration
	configurationListeners []func(dynamic.Configuration)

	configurationTransformers []func(context.Context, dynamic.Configurations) dynamic.Configurations

	routinesPool *safe.Pool
}

// NewConfigurationWatcher creates a new ConfigurationWatcher.
func NewConfigurationWatcher(
	routinesPool *safe.Pool,
	pvd provider.Provider,
	defaultEntryPoints []string,
	requiredProvider string,
	strictTLSOptions bool,
) *ConfigurationWatcher {
	return &ConfigurationWatcher{
		providerAggregator:  pvd,
		allProvidersConfigs: make(chan dynamic.Message, 100),
		newConfigs:          make(chan dynamic.Configurations),
		routinesPool:        routinesPool,
		defaultEntryPoints:  defaultEntryPoints,
		requiredProvider:    requiredProvider,
		providersTimeout:    defaultProvidersTimeout,
		strictTLSOptions:    strictTLSOptions,
	}
}

// Start the configuration watcher.
func (c *ConfigurationWatcher) Start() {
	c.routinesPool.GoCtx(c.receiveConfigurations)
	c.routinesPool.GoCtx(c.applyConfigurations)
	c.startProviderAggregator()
}

// Stop the configuration watcher.
func (c *ConfigurationWatcher) Stop() {
	close(c.allProvidersConfigs)
	close(c.newConfigs)
}

// AddListener adds a new listener function used when new configuration is provided.
func (c *ConfigurationWatcher) AddListener(listener func(dynamic.Configuration)) {
	c.configurationListeners = append(c.configurationListeners, listener)
}

// AddTransformer registers a function to modify configurations before they are applied.
func (c *ConfigurationWatcher) AddTransformer(transformer func(context.Context, dynamic.Configurations) dynamic.Configurations) {
	c.configurationTransformers = append(c.configurationTransformers, transformer)
}

func (c *ConfigurationWatcher) startProviderAggregator() {
	log.Info().Msgf("Starting provider aggregator %T", c.providerAggregator)

	safe.Go(func() {
		err := c.providerAggregator.Provide(c.allProvidersConfigs, c.routinesPool)
		if err != nil {
			log.Error().Err(err).Msgf("Error starting provider aggregator %T", c.providerAggregator)
		}
	})
}

// receiveConfigurations receives configuration changes from the providers.
// The configuration message then gets passed along a series of checks, notably
// to verify that, for a given provider, the configuration that was just received
// is at least different from the previously received one.
// The full set of configurations is then sent to applyConfigurations
// via a channel in a non-blocking manner, ensuring the latest global state
// is always available for processing.
//
// The very first set is held back until the providers the configuration refers to
// answered, so that it is applied with all the resources the routers need.
func (c *ConfigurationWatcher) receiveConfigurations(ctx context.Context) {
	newConfigurations := make(dynamic.Configurations)

	// The providers which sent at least one configuration, hence which are able to
	// resolve the references made to their resources.
	answeredProviders := make(map[string]struct{})

	var output chan dynamic.Configurations

	var pending dynamic.Configurations

	var held dynamic.Configurations
	var timeout *time.Timer
	var timeoutC <-chan time.Time

	awaitingProviders := true

	// dispatch hands the given configurations over to be applied, unless they are the
	// first ones and that a provider they refer to did not answer yet: they are then
	// held back, and dispatched later on, when the missing providers answer, or when
	// the timeout expires.
	dispatch := func(logger zerolog.Logger, configurations dynamic.Configurations) {
		if awaitingProviders {
			if missing := c.missingProviders(configurations, answeredProviders); len(missing) > 0 {
				logger.Debug().Strs(logs.ProviderName, missing).Msg("Waiting for the first configuration of these providers")

				held = configurations
				if timeout == nil {
					timeout = time.NewTimer(c.providersTimeout)
					timeoutC = timeout.C
				}
				return
			}

			awaitingProviders = false
		}

		if timeout != nil {
			timeout.Stop()
			timeout, timeoutC = nil, nil
		}

		held = nil
		output = c.newConfigs
		pending = configurations
	}

	// reconsider dispatches the held configurations, when the provider which was waited
	// for answered with a configuration which was skipped.
	reconsider := func(logger zerolog.Logger) {
		if held != nil {
			dispatch(logger, held)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case output <- pending:
			output = nil
			pending = nil

		default:
			select {
			case <-ctx.Done():
				return

			case <-timeoutC:
				log.Warn().
					Strs(logs.ProviderName, c.missingProviders(held, answeredProviders)).
					Msg("Timeout while waiting for the first configuration of these providers, applying the configuration anyway")

				timeout, timeoutC = nil, nil
				awaitingProviders = false

				output = c.newConfigs
				pending = held
				held = nil

			case configMsg, ok := <-c.allProvidersConfigs:
				if !ok {
					return
				}

				logger := log.Ctx(ctx).With().Str(logs.ProviderName, configMsg.ProviderName).Logger()

				answeredProviders[configMsg.ProviderName] = struct{}{}

				if configMsg.Configuration == nil {
					logger.Debug().Msg("Skipping nil configuration")
					reconsider(logger)
					continue
				}

				if isEmptyConfiguration(configMsg.Configuration) {
					logger.Debug().Msg("Skipping empty configuration")
					reconsider(logger)
					continue
				}

				logConfiguration(logger, configMsg)

				if reflect.DeepEqual(newConfigurations[configMsg.ProviderName], configMsg.Configuration) {
					// no change, do nothing.
					logger.Debug().Msg("Skipping unchanged configuration")
					reconsider(logger)
					continue
				}

				newConfigurations[configMsg.ProviderName] = configMsg.Configuration.DeepCopy()

				// DeepCopy is necessary because newConfigurations gets modified later by the consumer of c.newConfigs.
				transformedConfigurations := newConfigurations.DeepCopy()
				for _, transform := range c.configurationTransformers {
					// Each transformer gets its own copy because a transformer could keep a reference to the one it received.
					transformedConfigurations = transform(logger.WithContext(ctx), transformedConfigurations)
					transformedConfigurations = transformedConfigurations.DeepCopy()
				}

				dispatch(logger, transformedConfigurations)

			case output <- pending:
				output = nil
				pending = nil
			}
		}
	}
}

// applyConfigurations receives the full set of configurations from
// receiveConfigurations and applies them if they differ from the previous set.
// It waits for the required provider's configuration before applying any configs.
func (c *ConfigurationWatcher) applyConfigurations(ctx context.Context) {
	var lastConfigurations dynamic.Configurations
	for {
		select {
		case <-ctx.Done():
			return
		case newConfigs, ok := <-c.newConfigs:
			if !ok {
				return
			}

			// We wait for first configuration of the required provider before applying configurations.
			if _, ok := newConfigs[c.requiredProvider]; c.requiredProvider != "" && !ok {
				continue
			}

			if reflect.DeepEqual(newConfigs, lastConfigurations) {
				continue
			}

			conf := mergeConfiguration(newConfigs.DeepCopy(), c.defaultEntryPoints)
			conf = applyModel(conf)
			if conf.HTTP != nil {
				conf.HTTP.Routers = resolveHTTPTLSOptions(conf.HTTP.Routers, c.strictTLSOptions)
			}

			for _, listener := range c.configurationListeners {
				listener(conf)
			}

			lastConfigurations = newConfigs
		}
	}
}

// missingProviders returns the names, sorted, of the providers which the given
// configurations refer to, and which did not send any configuration yet, among them the
// required provider, which must be loaded for the configuration to be worth applying.
func (c *ConfigurationWatcher) missingProviders(configurations dynamic.Configurations, answeredProviders map[string]struct{}) []string {
	var missing []string

	if c.requiredProvider != "" {
		if _, ok := answeredProviders[c.requiredProvider]; !ok {
			missing = append(missing, c.requiredProvider)
		}
	}

	for _, providerName := range referencedProviders(configurations) {
		if _, ok := answeredProviders[providerName]; !ok && !slices.Contains(missing, providerName) {
			missing = append(missing, providerName)
		}
	}

	slices.Sort(missing)

	return missing
}

// referencedProviders returns, sorted, the names of the providers which the resources of
// the given configurations refer to, other than the provider which provides them.
func referencedProviders(configurations dynamic.Configurations) []string {
	collector := &providerCollector{names: map[string]struct{}{}}

	for providerName, configuration := range configurations {
		if configuration == nil {
			continue
		}

		collector.provider = providerName
		collectReferences(collector, configuration)
	}

	references := make([]string, 0, len(collector.names))
	for name := range collector.names {
		references = append(references, name)
	}
	slices.Sort(references)

	return references
}

// providerCollector gathers the providers referred to by the resources of a single
// provider.
type providerCollector struct {
	provider string
	names    map[string]struct{}
}

// add collects the provider of a qualified resource name. An unqualified name, like a
// name qualified with the provider itself, is resolved by the provider which provides the
// resource, and is not a dependency on another provider.
func (p *providerCollector) add(reference string) {
	parts := strings.Split(reference, "@")
	if len(parts) < 2 {
		return
	}

	if providerName := parts[1]; providerName != "" && providerName != p.provider {
		p.names[providerName] = struct{}{}
	}
}

func (p *providerCollector) addAll(references []string) {
	for _, reference := range references {
		p.add(reference)
	}
}

func collectReferences(collector *providerCollector, configuration *dynamic.Configuration) {
	if configuration.HTTP != nil {
		for _, router := range configuration.HTTP.Routers {
			collector.addAll(router.Middlewares)
			collector.add(router.Service)
			collector.addAll(router.ParentRefs)

			if router.TLS != nil {
				collector.add(router.TLS.Options)
			}
		}

		// The entryPoint middlewares are provided by the internal provider, through the
		// models, and are applied to every router of the entryPoint.
		for _, model := range configuration.HTTP.Models {
			collector.addAll(model.Middlewares)

			if model.TLS != nil {
				collector.add(model.TLS.Options)
			}
		}

		for _, service := range configuration.HTTP.Services {
			collector.addAll(service.Middlewares)

			if service.LoadBalancer != nil {
				collector.add(service.LoadBalancer.ServersTransport)
			}

			if service.Weighted != nil {
				collector.addAll(serviceNames(service.Weighted.Services, func(service dynamic.WRRService) string { return service.Name }))
			}

			if service.HighestRandomWeight != nil {
				collector.addAll(serviceNames(service.HighestRandomWeight.Services, func(service dynamic.HRWService) string { return service.Name }))
			}

			if service.Mirroring != nil {
				collector.add(service.Mirroring.Service)
				collector.addAll(serviceNames(service.Mirroring.Mirrors, func(service dynamic.MirrorService) string { return service.Name }))
			}

			if service.Failover != nil {
				collector.add(service.Failover.Service)
				collector.add(service.Failover.Fallback)
			}
		}

		for _, middleware := range configuration.HTTP.Middlewares {
			if middleware.Chain != nil {
				collector.addAll(middleware.Chain.Middlewares)
			}

			if middleware.Errors != nil {
				collector.add(middleware.Errors.Service)
			}
		}
	}

	if configuration.TCP != nil {
		for _, router := range configuration.TCP.Routers {
			collector.addAll(router.Middlewares)
			collector.add(router.Service)

			if router.TLS != nil {
				collector.add(router.TLS.Options)
			}
		}

		for _, service := range configuration.TCP.Services {
			if service.LoadBalancer != nil {
				collector.add(service.LoadBalancer.ServersTransport)
			}

			if service.Weighted != nil {
				collector.addAll(serviceNames(service.Weighted.Services, func(service dynamic.TCPWRRService) string { return service.Name }))
			}
		}
	}

	if configuration.UDP != nil {
		for _, router := range configuration.UDP.Routers {
			collector.add(router.Service)
		}

		for _, service := range configuration.UDP.Services {
			if service.Weighted != nil {
				collector.addAll(serviceNames(service.Weighted.Services, func(service dynamic.UDPWRRService) string { return service.Name }))
			}
		}
	}
}

// serviceNames returns the referenced names of the given services.
func serviceNames[T any](services []T, name func(T) string) []string {
	if len(services) == 0 {
		return nil
	}

	references := make([]string, 0, len(services))
	for _, service := range services {
		references = append(references, name(service))
	}

	return references
}

func logConfiguration(logger zerolog.Logger, configMsg dynamic.Message) {
	if logger.GetLevel() > zerolog.DebugLevel {
		return
	}

	copyConf := configMsg.Configuration.DeepCopy()
	if copyConf.TLS != nil {
		copyConf.TLS.Certificates = nil

		if copyConf.TLS.Options != nil {
			cleanedOptions := make(map[string]tls.Options, len(copyConf.TLS.Options))
			for name, option := range copyConf.TLS.Options {
				option.ClientAuth.CAFiles = []types.FileOrContent{}
				cleanedOptions[name] = option
			}

			copyConf.TLS.Options = cleanedOptions
		}

		for k := range copyConf.TLS.Stores {
			st := copyConf.TLS.Stores[k]
			st.DefaultCertificate = nil
			copyConf.TLS.Stores[k] = st
		}
	}

	if copyConf.HTTP != nil {
		for _, transport := range copyConf.HTTP.ServersTransports {
			transport.Certificates = tls.Certificates{}
			transport.RootCAs = []types.FileOrContent{}
		}
	}

	if copyConf.TCP != nil {
		for _, transport := range copyConf.TCP.ServersTransports {
			if transport.TLS != nil {
				transport.TLS.Certificates = tls.Certificates{}
				transport.TLS.RootCAs = []types.FileOrContent{}
			}
		}
	}

	jsonConf, err := json.Marshal(copyConf)
	if err != nil {
		logger.Error().Err(err).Msg("Could not marshal dynamic configuration")
		logger.Debug().Msgf("Configuration received: [struct] %#v", copyConf)
	} else {
		logger.Debug().RawJSON("config", jsonConf).Msg("Configuration received")
	}
}

func isEmptyConfiguration(conf *dynamic.Configuration) bool {
	if conf.TCP == nil {
		conf.TCP = &dynamic.TCPConfiguration{}
	}
	if conf.HTTP == nil {
		conf.HTTP = &dynamic.HTTPConfiguration{}
	}
	if conf.UDP == nil {
		conf.UDP = &dynamic.UDPConfiguration{}
	}

	httpEmpty := conf.HTTP.Routers == nil && conf.HTTP.Services == nil && conf.HTTP.Middlewares == nil
	tlsEmpty := conf.TLS == nil || conf.TLS.Certificates == nil && conf.TLS.Stores == nil && conf.TLS.Options == nil
	tcpEmpty := conf.TCP.Routers == nil && conf.TCP.Services == nil && conf.TCP.Middlewares == nil
	udpEmpty := conf.UDP.Routers == nil && conf.UDP.Services == nil

	return httpEmpty && tlsEmpty && tcpEmpty && udpEmpty
}
