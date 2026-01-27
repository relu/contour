// Copyright Project Contour Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v3

import (
	"fmt"
	"sort"
	"sync"

	envoy_config_core_v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_config_endpoint_v3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	core_v1 "k8s.io/api/core/v1"
	discovery_v1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/projectcontour/contour/internal/contour"
	"github.com/projectcontour/contour/internal/dag"
	envoy_v3 "github.com/projectcontour/contour/internal/envoy/v3"
	"github.com/projectcontour/contour/internal/k8s"
	"github.com/projectcontour/contour/internal/protobuf"
	"github.com/projectcontour/contour/internal/sorter"
)

type (
	LocalityEndpoints     = envoy_config_endpoint_v3.LocalityLbEndpoints
	LoadBalancingEndpoint = envoy_config_endpoint_v3.LbEndpoint
)

// ZoneAwareRoutingConfig holds zone-aware routing settings for the translator.
type ZoneAwareRoutingConfig struct {
	Enabled bool
}

// localityEndpointGroup groups endpoints by their zone.
// Envoy will use its own locality to prefer endpoints in the same zone.
type localityEndpointGroup struct {
	zone      string
	endpoints []*LoadBalancingEndpoint
}

// RecalculateEndpoints generates a slice of LoadBalancingEndpoint
// resources by matching the given service port to the given discovery_v1.EndpointSlice.
// endpointSliceMap may be nil, in which case, the result is also nil.
func (c *EndpointSliceCache) RecalculateEndpoints(port, healthPort core_v1.ServicePort, endpointSliceMap map[string]*discovery_v1.EndpointSlice) []*LoadBalancingEndpoint {
	// Delegate to recalculateEndpointsWithLocality with zone-aware routing disabled.
	// This returns a single group with all endpoints.
	groups := c.recalculateEndpointsWithLocality(port, healthPort, endpointSliceMap, ZoneAwareRoutingConfig{Enabled: false})
	if len(groups) == 0 {
		return nil
	}
	// Flatten all groups into a single slice (there should be only one group when ZAR is disabled).
	var lb []*LoadBalancingEndpoint
	for _, group := range groups {
		lb = append(lb, group.endpoints...)
	}
	return lb
}

// recalculateEndpointsWithLocality generates endpoint groups by zone.
// When zone-aware routing is disabled, all endpoints are returned in a single group with no zone.
// When zone-aware routing is enabled, endpoints are grouped by their zone (from endpoint.Zone).
// Envoy will use its own configured locality to prefer endpoints in the same zone.
func (c *EndpointSliceCache) recalculateEndpointsWithLocality(port, healthPort core_v1.ServicePort, endpointSliceMap map[string]*discovery_v1.EndpointSlice, zarConfig ZoneAwareRoutingConfig) []localityEndpointGroup {
	// Map from zone -> endpoints in that zone
	// When ZAR is disabled, all endpoints go to empty zone ""
	zoneEndpoints := make(map[string][]*LoadBalancingEndpoint)
	uniqueEndpoints := make(map[string]struct{})
	var healthCheckPort int32

	for _, endpointSlice := range endpointSliceMap {
		sort.Slice(endpointSlice.Endpoints, func(i, j int) bool {
			return endpointSlice.Endpoints[i].Addresses[0] < endpointSlice.Endpoints[j].Addresses[0]
		})

		for _, endpoint := range endpointSlice.Endpoints {
			// Skip if the endpointSlice is not marked as ready.
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				continue
			}

			// Get the zone for this endpoint.
			// When ZAR is disabled, all endpoints go to empty zone.
			// When ZAR is enabled, use the endpoint's zone (empty string if not set).
			zone := ""
			if zarConfig.Enabled && endpoint.Zone != nil {
				zone = *endpoint.Zone
			}

			// Range over each port. We want the resultant endpoints to be a
			// a cartesian product MxN where M are the endpoints and N are the ports.
			for _, endpointPort := range endpointSlice.Ports {
				// Nil check for the port.
				if endpointPort.Port == nil {
					continue
				}
				if endpointPort.Protocol == nil {
					continue
				}
				if *endpointPort.Protocol != core_v1.ProtocolTCP {
					continue
				}

				// Set healthCheckPort only when port and healthPort are different.
				if endpointPort.Name != nil && (healthPort.Name != "" && healthPort.Name == *endpointPort.Name && port.Name != healthPort.Name) {
					healthCheckPort = *endpointPort.Port
				}

				// Match by port name.
				if port.Name != "" && endpointPort.Name != nil && port.Name != *endpointPort.Name {
					continue
				}

				// we can safely take the first element here.
				// Refer k8s API description:
				// The contents of this field are interpreted according to
				// the corresponding EndpointSlice addressType field.
				// Consumers must handle different types of addresses in the context
				// of their own capabilities. This must contain at least one
				// address but no more than 100. These are all assumed to be fungible
				// and clients may choose to only use the first element.
				// Refer to: https://issue.k8s.io/106267
				addr := envoy_v3.SocketAddress(endpoint.Addresses[0], int(*endpointPort.Port))

				// as per note on https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/
				// Clients of the EndpointSlice API must iterate through all the existing EndpointSlices associated to
				// a Service and build a complete list of unique network endpoints. It is important to mention that
				// endpoints may be duplicated in different EndpointSlices.
				// Hence, we need to ensure that the endpoints we add to []*LoadBalancingEndpoint aren't duplicated.
				endpointKey := fmt.Sprintf("%s:%d", endpoint.Addresses[0], *endpointPort.Port)
				if _, exists := uniqueEndpoints[endpointKey]; exists {
					continue
				}
				uniqueEndpoints[endpointKey] = struct{}{}

				lbEndpoint := envoy_v3.LBEndpoint(addr)
				zoneEndpoints[zone] = append(zoneEndpoints[zone], lbEndpoint)
			}
		}
	}

	// Apply health check config to all endpoints
	if healthCheckPort > 0 {
		for _, endpoints := range zoneEndpoints {
			for _, lbEndpoint := range endpoints {
				lbEndpoint.GetEndpoint().HealthCheckConfig = envoy_v3.HealthCheckConfig(healthCheckPort)
			}
		}
	}

	// Convert map to sorted slice of groups for deterministic output
	var result []localityEndpointGroup
	zones := make([]string, 0, len(zoneEndpoints))
	for zone := range zoneEndpoints {
		zones = append(zones, zone)
	}
	sort.Strings(zones)

	for _, zone := range zones {
		if len(zoneEndpoints[zone]) > 0 {
			result = append(result, localityEndpointGroup{
				zone:      zone,
				endpoints: zoneEndpoints[zone],
			})
		}
	}

	return result
}

// EndpointSliceCache is a cache of EndpointSlice and ServiceCluster objects.
type EndpointSliceCache struct {
	mu sync.Mutex // Protects all fields.

	// Slice of stale clusters. A stale cluster is one that
	// needs to be recalculated. Clusters can be added to the stale
	// slice due to changes in EndpointSlices or due to a DAG rebuild.
	stale []*dag.ServiceCluster

	// Index of ServiceClusters. ServiceClusters are indexed
	// by the name of their Kubernetes Services. This makes it
	// easy to determine which Endpoints affect which ServiceCluster.
	services map[types.NamespacedName][]*dag.ServiceCluster

	// Cache of endpointsSlices, indexed by Namespaced name of the associated service.
	// the Inner map is a map[k,v] where k is the endpoint slice name and v is the
	// endpoint slice itself.
	endpointSlices map[types.NamespacedName]map[string]*discovery_v1.EndpointSlice
}

// Recalculate regenerates all the ClusterLoadAssignments from the
// cached EndpointSlices and stale ServiceClusters. A ClusterLoadAssignment
// will be generated for every stale ServerCluster, however, if there
// are no endpointSlices for the Services in the ServiceCluster, the
// ClusterLoadAssignment will be empty.
func (c *EndpointSliceCache) Recalculate(zarConfig ZoneAwareRoutingConfig) map[string]*envoy_config_endpoint_v3.ClusterLoadAssignment {
	c.mu.Lock()
	defer c.mu.Unlock()

	assignments := map[string]*envoy_config_endpoint_v3.ClusterLoadAssignment{}
	for _, cluster := range c.stale {
		// Clusters can be in the stale list multiple times;
		// skip to avoid duplicate recalculations.
		if _, ok := assignments[cluster.ClusterName]; ok {
			continue
		}

		cla := envoy_config_endpoint_v3.ClusterLoadAssignment{
			ClusterName: cluster.ClusterName,
			Endpoints:   nil,
			Policy:      nil,
		}

		// Look up each service, and if we have endpointSlice for that service,
		// attach them as LocalityEndpoints resources grouped by zone.
		for _, w := range cluster.Services {
			n := types.NamespacedName{Namespace: w.ServiceNamespace, Name: w.ServiceName}
			groups := c.recalculateEndpointsWithLocality(w.ServicePort, w.HealthPort, c.endpointSlices[n], zarConfig)

			for _, group := range groups {
				if len(group.endpoints) > 0 {
					localityLbEndpoints := &LocalityEndpoints{
						LbEndpoints:         group.endpoints,
						LoadBalancingWeight: protobuf.UInt32OrNil(w.Weight),
					}

					// Set locality if ZAR is enabled and we have zone info
					if zarConfig.Enabled && group.zone != "" {
						localityLbEndpoints.Locality = &envoy_config_core_v3.Locality{
							Zone: group.zone,
						}
					}

					cla.Endpoints = append(cla.Endpoints, localityLbEndpoints)
				}
			}
		}

		assignments[cla.ClusterName] = &cla
	}

	c.stale = nil
	return assignments
}

// SetClusters replaces the cache of ServiceCluster resources. All
// the added clusters will be marked stale.
func (c *EndpointSliceCache) SetClusters(clusters []*dag.ServiceCluster) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Keep a local index to start with so that errors don't cause
	// partial failure.
	serviceIndex := map[types.NamespacedName][]*dag.ServiceCluster{}

	// Reindex the cluster so that we can find them by service name.
	for _, cluster := range clusters {
		if err := cluster.Validate(); err != nil {
			return fmt.Errorf("invalid ServiceCluster %q: %w", cluster.ClusterName, err)
		}

		// Make sure service clusters with default weights are balanced.
		cluster.Rebalance()

		for _, s := range cluster.Services {
			name := types.NamespacedName{
				Namespace: s.ServiceNamespace,
				Name:      s.ServiceName,
			}

			// Create the slice entry if we have not indexed this service yet.
			entry := serviceIndex[name]
			if entry == nil {
				entry = []*dag.ServiceCluster{}
			}

			serviceIndex[name] = append(entry, cluster)
		}
	}

	c.stale = clusters
	c.services = serviceIndex

	return nil
}

// UpdateEndpointSlice adds endpointSlice to the cache, or replaces it if it is
// already cached. Any ServiceClusters that are backed by a Service
// that endpointSlice belongs become stale. Returns a boolean indicating whether
// any ServiceClusters use endpointSlice or not.
func (c *EndpointSliceCache) UpdateEndpointSlice(endpointSlice *discovery_v1.EndpointSlice) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	name := types.NamespacedName{Namespace: endpointSlice.Namespace, Name: endpointSlice.Labels[discovery_v1.LabelServiceName]}

	if c.endpointSlices[name] == nil {
		c.endpointSlices[name] = make(map[string]*discovery_v1.EndpointSlice)
	}
	c.endpointSlices[name][endpointSlice.Name] = endpointSlice.DeepCopy()

	// If any service clusters include this endpointSlice, mark them
	// all as stale.
	if affected := c.services[name]; len(affected) > 0 {
		c.stale = append(c.stale, affected...)
		return true
	}

	return false
}

// DeleteEndpointSlice deletes endpointSlice from the cache. Any ServiceClusters
// that are backed by a Service that endpointSlice belongs to, become stale. Returns
// a boolean indicating whether any ServiceClusters use endpointSlice or not.
func (c *EndpointSliceCache) DeleteEndpointSlice(endpointSlice *discovery_v1.EndpointSlice) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	name := types.NamespacedName{Namespace: endpointSlice.Namespace, Name: endpointSlice.Labels[discovery_v1.LabelServiceName]}
	delete(c.endpointSlices[name], endpointSlice.Name)

	// If any service clusters include this endpointSlice, mark them
	// all as stale.
	if affected := c.services[name]; len(affected) > 0 {
		c.stale = append(c.stale, affected...)
		return true
	}

	return false
}

// NewEndpointSliceTranslator allocates a new endpointsSlice translator.
func NewEndpointSliceTranslator(log logrus.FieldLogger, zarConfig ZoneAwareRoutingConfig) *EndpointSliceTranslator {
	return &EndpointSliceTranslator{
		FieldLogger: log,
		zarConfig:   zarConfig,
		entries:     map[string]*envoy_config_endpoint_v3.ClusterLoadAssignment{},
		cache: EndpointSliceCache{
			stale:          nil,
			services:       map[types.NamespacedName][]*dag.ServiceCluster{},
			endpointSlices: map[types.NamespacedName]map[string]*discovery_v1.EndpointSlice{},
		},
	}
}

// A EndpointsSliceTranslator translates Kubernetes EndpointSlice objects into Envoy
// ClusterLoadAssignment resources.
type EndpointSliceTranslator struct {
	// Observer notifies when the endpointSlice cache has been updated.
	Observer contour.Observer

	logrus.FieldLogger

	// zarConfig holds zone-aware routing settings.
	zarConfig ZoneAwareRoutingConfig

	cache EndpointSliceCache

	mu      sync.Mutex // Protects entries.
	entries map[string]*envoy_config_endpoint_v3.ClusterLoadAssignment
}

// Merge combines the given entries with the existing entries in the
// EndpointSliceTranslator. If the same key exists in both maps, an existing entry
// is replaced.
func (e *EndpointSliceTranslator) Merge(entries map[string]*envoy_config_endpoint_v3.ClusterLoadAssignment) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k, v := range entries {
		e.entries[k] = v
	}
}

// OnChange observes DAG rebuild events.
func (e *EndpointSliceTranslator) OnChange(root *dag.DAG) {
	clusters := []*dag.ServiceCluster{}
	names := map[string]bool{}

	for _, svc := range root.GetServiceClusters() {
		if err := svc.Validate(); err != nil {
			e.WithError(err).Errorf("dropping invalid service cluster %q", svc.ClusterName)
		} else if _, ok := names[svc.ClusterName]; ok {
			e.Debugf("dropping service cluster with duplicate name %q", svc.ClusterName)
		} else {
			e.Debugf("added ServiceCluster %q from DAG", svc.ClusterName)
			clusters = append(clusters, svc.DeepCopy())
			names[svc.ClusterName] = true
		}
	}

	// Update the cache with the new clusters.
	if err := e.cache.SetClusters(clusters); err != nil {
		e.WithError(err).Error("failed to cache service clusters")
	}

	// After rebuilding the DAG, the service cluster could be
	// completely different. Some could be added, and some could
	// be removed. Since we reset the cluster cache above, all
	// the load assignments will be recalculated and we can just
	// set the entries rather than merging them.
	e.mu.Lock()
	e.entries = e.cache.Recalculate(e.zarConfig)
	e.mu.Unlock()

	if e.Observer != nil {
		e.Observer.Refresh()
	}
}

// equal returns true if a and b are the same length, have the same set
// of keys, and have proto-equivalent values for each key, or false otherwise.
func equal(a, b map[string]*envoy_config_endpoint_v3.ClusterLoadAssignment) bool {
	if len(a) != len(b) {
		return false
	}

	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}

		if !proto.Equal(a[k], b[k]) {
			return false
		}
	}

	return true
}

func (e *EndpointSliceTranslator) OnAdd(obj any, _ bool) {
	switch obj := obj.(type) {
	case *discovery_v1.EndpointSlice:
		if !e.cache.UpdateEndpointSlice(obj) {
			return
		}

		e.WithField("endpointSlice", k8s.NamespacedNameOf(obj)).Debug("EndpointSlice is in use by a ServiceCluster, recalculating ClusterLoadAssignments")
		e.Merge(e.cache.Recalculate(e.zarConfig))
		if e.Observer != nil {
			e.Observer.Refresh()
		}
	default:
		e.Errorf("OnAdd unexpected type %T: %#v", obj, obj)
	}
}

func (e *EndpointSliceTranslator) OnUpdate(oldObj, newObj any) {
	switch newObj := newObj.(type) {
	case *discovery_v1.EndpointSlice:
		oldObj, ok := oldObj.(*discovery_v1.EndpointSlice)
		if !ok {
			e.Errorf("OnUpdate endpointSlice %#v received invalid oldObj %T; %#v", newObj, oldObj, oldObj)
			return
		}

		// Skip computation if either old and new services or
		// endpointSlice are equal (thus also handling nil).
		if oldObj == newObj {
			return
		}

		// If there are no endpointSlice in this object, and the old
		// object also had zero endpointSlice, ignore this update
		// to avoid sending a noop notification to watchers.
		if len(oldObj.Endpoints) == 0 && len(newObj.Endpoints) == 0 {
			return
		}

		if !e.cache.UpdateEndpointSlice(newObj) {
			return
		}

		e.WithField("endpointSlice", k8s.NamespacedNameOf(newObj)).Debug("EndpointSlice is in use by a ServiceCluster, recalculating ClusterLoadAssignments")
		e.Merge(e.cache.Recalculate(e.zarConfig))
		if e.Observer != nil {
			e.Observer.Refresh()
		}
	default:
		e.Errorf("OnUpdate unexpected type %T: %#v", newObj, newObj)
	}
}

func (e *EndpointSliceTranslator) OnDelete(obj any) {
	switch obj := obj.(type) {
	case *discovery_v1.EndpointSlice:
		if !e.cache.DeleteEndpointSlice(obj) {
			return
		}

		e.WithField("endpointSlice", k8s.NamespacedNameOf(obj)).Debug("EndpointSlice was in use by a ServiceCluster, recalculating ClusterLoadAssignments")
		e.Merge(e.cache.Recalculate(e.zarConfig))
		if e.Observer != nil {
			e.Observer.Refresh()
		}
	case cache.DeletedFinalStateUnknown:
		e.OnDelete(obj.Obj) // recurse into ourselves with the tombstoned value
	default:
		e.Errorf("OnDelete unexpected type %T: %#v", obj, obj)
	}
}

// Contents returns a copy of the contents of the cache.
func (e *EndpointSliceTranslator) Contents() []proto.Message {
	e.mu.Lock()
	defer e.mu.Unlock()

	values := make([]*envoy_config_endpoint_v3.ClusterLoadAssignment, 0, len(e.entries))
	for _, v := range e.entries {
		values = append(values, v)
	}

	sort.Stable(sorter.For(values))
	return protobuf.AsMessages(values)
}

func (*EndpointSliceTranslator) TypeURL() string { return resource.EndpointType }

func (e *EndpointSliceTranslator) SetObserver(observer contour.Observer) { e.Observer = observer }
