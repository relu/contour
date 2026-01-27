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

//go:build e2e

package infra

import (
	"encoding/json"
	"slices"
	"sort"

	envoy_admin_v3 "github.com/envoyproxy/go-control-plane/envoy/admin/v3"
	envoy_config_cluster_v3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	. "github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	contour_v1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	"github.com/projectcontour/contour/test/e2e"
)

func testSimpleEndpointSlice(namespace string) {
	Specify("test endpoint slices", func() {
		f.Fixtures.Echo.DeployN(namespace, "echo", 1)

		p := &contour_v1.HTTPProxy{
			ObjectMeta: meta_v1.ObjectMeta{
				Namespace: namespace,
				Name:      "endpoint-slice",
			},
			Spec: contour_v1.HTTPProxySpec{
				VirtualHost: &contour_v1.VirtualHost{
					Fqdn: "eps.projectcontour.io",
				},
				Routes: []contour_v1.Route{
					{
						Conditions: []contour_v1.MatchCondition{
							{
								Prefix: "/",
							},
						},
						Services: []contour_v1.Service{
							{
								Name: "echo",
								Port: 80,
							},
						},
					},
				},
			},
		}

		require.True(f.T(), f.CreateHTTPProxyAndWaitFor(p, e2e.HTTPProxyValid))

		require.Eventually(f.T(), func() bool {
			return IsEnvoyProgrammedWithAllPodIPs(namespace)
		}, f.RetryTimeout, f.RetryInterval)

		// scale up to 10 pods
		f.Fixtures.Echo.ScaleAndWaitDeployment("echo", namespace, 10)

		require.Eventually(f.T(), func() bool {
			return IsEnvoyProgrammedWithAllPodIPs(namespace)
		}, f.RetryTimeout, f.RetryInterval)

		// scale down to 2 pods
		f.Fixtures.Echo.ScaleAndWaitDeployment("echo", namespace, 2)

		require.Eventually(f.T(), func() bool {
			return IsEnvoyProgrammedWithAllPodIPs(namespace)
		}, f.RetryTimeout, f.RetryInterval)

		// scale to 0
		f.Fixtures.Echo.ScaleAndWaitDeployment("echo", namespace, 0)

		require.Eventually(f.T(), func() bool {
			return IsEnvoyProgrammedWithAllPodIPs(namespace)
		}, f.RetryTimeout, f.RetryInterval)
	})
}

func IsEnvoyProgrammedWithAllPodIPs(namespace string) bool {
	k8sPodIPs, err := f.Fixtures.Echo.ListPodIPs(namespace, "echo")
	if err != nil {
		return false
	}

	envoyEndpoints, err := GetIPsFromAdminRequest()
	if err != nil {
		return false
	}

	sort.Strings(k8sPodIPs)
	sort.Strings(envoyEndpoints)

	return slices.Equal(k8sPodIPs, envoyEndpoints)
}

// GetIPsFromAdminRequest makes a call to the envoy admin endpoint and parses
// all the IPs as a list from the echo cluster
func GetIPsFromAdminRequest() ([]string, error) {
	resp, _ := f.HTTP.AdminRequestUntil(&e2e.HTTPRequestOpts{
		Path:      "/clusters?format=json",
		Condition: e2e.HasStatusCode(200),
	})

	ips := make([]string, 0)

	clusters := &envoy_admin_v3.Clusters{}
	err := protojson.Unmarshal(resp.Body, clusters)
	if err != nil {
		return nil, err
	}

	for _, cluster := range clusters.ClusterStatuses {
		if cluster.Name == "simple-endpoint-slice/echo/80/da39a3ee5e" {
			for _, host := range cluster.HostStatuses {
				ips = append(ips, host.Address.GetSocketAddress().Address)
			}
		}
	}

	return ips, nil
}

func testZoneAwareRouting(namespace string) {
	Specify("zone-aware routing sets locality on endpoints", func() {
		f.Fixtures.Echo.DeployN(namespace, "echo", 2)

		p := &contour_v1.HTTPProxy{
			ObjectMeta: meta_v1.ObjectMeta{
				Namespace: namespace,
				Name:      "zar-endpoint-slice",
			},
			Spec: contour_v1.HTTPProxySpec{
				VirtualHost: &contour_v1.VirtualHost{
					Fqdn: "zar.projectcontour.io",
				},
				Routes: []contour_v1.Route{
					{
						Conditions: []contour_v1.MatchCondition{
							{
								Prefix: "/",
							},
						},
						Services: []contour_v1.Service{
							{
								Name: "echo",
								Port: 80,
							},
						},
					},
				},
			},
		}

		require.True(f.T(), f.CreateHTTPProxyAndWaitFor(p, e2e.HTTPProxyValid))

		// Verify that endpoints are programmed
		require.Eventually(f.T(), func() bool {
			return IsZAREnvoyProgrammedWithAllPodIPs(namespace)
		}, f.RetryTimeout, f.RetryInterval)

		// Verify locality information is set on endpoints.
		// When zone-aware routing is enabled and nodes have zone labels, endpoints should have locality set.
		// If nodes don't have zone labels, endpoints will still be programmed but without locality.
		require.Eventually(f.T(), VerifyZAREndpointsLocality, f.RetryTimeout, f.RetryInterval)

		// Verify that zone_aware_lb_config is set on clusters when zone-aware routing is enabled.
		// This is required for Envoy to perform zone-aware load balancing.
		clusterName := "zar-endpoint-slice/echo/80/da39a3ee5e"
		require.Eventually(f.T(), func() bool {
			return VerifyZoneAwareLBConfig(clusterName)
		}, f.RetryTimeout, f.RetryInterval)
	})
}

func IsZAREnvoyProgrammedWithAllPodIPs(namespace string) bool {
	k8sPodIPs, err := f.Fixtures.Echo.ListPodIPs(namespace, "echo")
	if err != nil {
		return false
	}

	envoyEndpoints, err := GetZARIPsFromAdminRequest()
	if err != nil {
		return false
	}

	sort.Strings(k8sPodIPs)
	sort.Strings(envoyEndpoints)

	return slices.Equal(k8sPodIPs, envoyEndpoints)
}

// GetZARIPsFromAdminRequest makes a call to the envoy admin endpoint and parses
// all the IPs as a list from the zar-endpoint-slice cluster
func GetZARIPsFromAdminRequest() ([]string, error) {
	resp, _ := f.HTTP.AdminRequestUntil(&e2e.HTTPRequestOpts{
		Path:      "/clusters?format=json",
		Condition: e2e.HasStatusCode(200),
	})

	ips := make([]string, 0)

	clusters := &envoy_admin_v3.Clusters{}
	err := protojson.Unmarshal(resp.Body, clusters)
	if err != nil {
		return nil, err
	}

	for _, cluster := range clusters.ClusterStatuses {
		if cluster.Name == "zar-endpoint-slice/echo/80/da39a3ee5e" {
			for _, host := range cluster.HostStatuses {
				ips = append(ips, host.Address.GetSocketAddress().Address)
			}
		}
	}

	return ips, nil
}

// VerifyZoneAwareLBConfig checks that when zone-aware routing is enabled, clusters have zone_aware_lb_config set.
// This is required for Envoy to perform zone-aware load balancing.
func VerifyZoneAwareLBConfig(clusterName string) bool {
	resp, _ := f.HTTP.AdminRequestUntil(&e2e.HTTPRequestOpts{
		Path:      "/config_dump?resource=dynamic_active_clusters",
		Condition: e2e.HasStatusCode(200),
	})

	// The config_dump response is a ConfigDump proto, but we only need to check
	// if zone_aware_lb_config is present in the cluster config
	var configDump struct {
		Configs []struct {
			Cluster json.RawMessage `json:"cluster"`
		} `json:"configs"`
	}

	if err := json.Unmarshal(resp.Body, &configDump); err != nil {
		return false
	}

	for _, config := range configDump.Configs {
		cluster := &envoy_config_cluster_v3.Cluster{}
		if err := protojson.Unmarshal(config.Cluster, cluster); err != nil {
			continue
		}

		if cluster.Name == clusterName {
			// Check if zone_aware_lb_config is set
			if cluster.CommonLbConfig == nil {
				return false
			}
			_, hasZoneAwareLB := cluster.CommonLbConfig.LocalityConfigSpecifier.(*envoy_config_cluster_v3.Cluster_CommonLbConfig_ZoneAwareLbConfig_)
			return hasZoneAwareLB
		}
	}

	return false
}

// VerifyZAREndpointsLocality checks that when zone-aware routing is enabled, endpoints have locality set
// (if their nodes have zone labels). Returns true if:
// - All endpoints are programmed, AND
// - Either all endpoints have locality set, OR no endpoints have locality set
// A mix of endpoints with and without locality would indicate a bug.
func VerifyZAREndpointsLocality() bool {
	resp, _ := f.HTTP.AdminRequestUntil(&e2e.HTTPRequestOpts{
		Path:      "/clusters?format=json",
		Condition: e2e.HasStatusCode(200),
	})

	clusters := &envoy_admin_v3.Clusters{}
	err := protojson.Unmarshal(resp.Body, clusters)
	if err != nil {
		return false
	}

	for _, cluster := range clusters.ClusterStatuses {
		if cluster.Name == "zar-endpoint-slice/echo/80/da39a3ee5e" {
			if len(cluster.HostStatuses) == 0 {
				return false
			}

			// Count hosts with locality set
			hostsWithLocality := 0
			for _, host := range cluster.HostStatuses {
				if host.Locality != nil && host.Locality.Zone != "" {
					hostsWithLocality++
				}
			}

			// Zone-aware routing is working if either:
			// 1. All hosts have locality set (nodes have zone labels), OR
			// 2. No hosts have locality set (nodes don't have zone labels - still works but falls back to no locality preference)
			// A mix would indicate a problem with the implementation.
			allHaveLocality := hostsWithLocality == len(cluster.HostStatuses)
			noneHaveLocality := hostsWithLocality == 0
			return allHaveLocality || noneHaveLocality
		}
	}

	return false
}
