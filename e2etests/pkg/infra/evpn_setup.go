// SPDX-License-Identifier:Apache-2.0

package infra

import (
	_ "embed"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/metallb/frrk8stests/pkg/k8s"
	"go.universe.tf/e2etest/pkg/executor"
	frrcontainer "go.universe.tf/e2etest/pkg/frr/container"
	corev1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
)

//go:embed data/evpn-node-setup.sh
var evpnNodeSetupScript string

// EVPNConfig holds the parameters for EVPN test networking setup. See
// data/evpn-node-setup.sh for more details on the networking infrastructure
// created on each target.
type EVPNConfig struct {
	// Bridge name of the VLAN-aware bridge where EVPNs are segmented
	// (e.g. "evpnbr").
	Bridge string
	// Vxlan name of the single VXLAN device carrying all VNIs in vnifilter mode
	// (e.g. "evpnvx").
	Vxlan string
	// L2VNI number for the L2 MAC-VRF (e.g. 1000). Zero disables it.
	L2VNI int
	// L2VLANID local VLAN ID segmenting the L2 MAC-VRF on the bridge (e.g.
	// 100).
	L2VLANID int
	// L2IPFmt pattern for L2 access port addresses, with a single %d for the
	// target index (e.g. "10.100.0.%d/24"). Use L2IP() to get the host IP for a
	// given index.
	L2IPFmt string
	// L3VNI number for L3 IP-VRF (e.g. 3000). Zero disables it.
	L3VNI int
	// L3VLANID local VLAN ID segmenting the L3 IP-VRF on the bridge (e.g.
	// 4000).
	L3VLANID int
	// L3VRF name of the Linux VRF for L3 L3 IP-VRF (e.g. "evpnred").
	L3VRF string
	// L3VRFTable number for the VRF.
	L3VRFTable int
	// L3PrefixFmt pattern for L3 VRF connected prefixes, with a single %d for
	// the target index (e.g. "10.200.%d.1/24"). Use L3Prefix() to get the
	// network CIDR or L3PrefixIP() to get the host IP.
	L3PrefixFmt string
}

// L2IP returns the L2 access port IP (without mask) for the given target index.
// Indices are 1-based: cluster nodes get indices 1..N matching their position
// in the nodes slice passed to SetupEVPN; the external FRR container gets
// index N+1.
func (c EVPNConfig) L2IP(index int) string {
	ip, _, _ := net.ParseCIDR(fmt.Sprintf(c.L2IPFmt, index))
	return ip.String()
}

// L3Prefix returns the L3 VRF connected network prefix (CIDR) for the given
// target index. See L2IP for index numbering.
func (c EVPNConfig) L3Prefix(index int) string {
	_, network, _ := net.ParseCIDR(fmt.Sprintf(c.L3PrefixFmt, index))
	return network.String()
}

// L3PrefixIP returns the L3 host IP (without mask) for the given target index.
// See L2IP for index numbering.
func (c EVPNConfig) L3PrefixIP(index int) string {
	ip, _, _ := net.ParseCIDR(fmt.Sprintf(c.L3PrefixFmt, index))
	return ip.String()
}

// SetupEVPN configures EVPN networking on cluster nodes and the external FRR
// container. On each target it runs data/evpn-node-setup.sh via the executor
// package to create the bridge, VXLAN, VNI and VRF infrastructure, then
// configures EVPN BGP on the external FRR via vtysh.
// See data/evpn-node-setup.sh for details on the per-target networking setup.
func SetupEVPN(cs clientset.Interface, nodes []corev1.Node, externalFRR *frrcontainer.FRR, cfg EVPNConfig) error {
	pods, err := k8s.FRRK8sPods(cs)
	if err != nil {
		return fmt.Errorf("listing frr-k8s pods: %w", err)
	}

	for i, node := range nodes {
		pod, err := podForNode(pods, node.Name)
		if err != nil {
			return err
		}
		podExec := executor.ForPod(pod.Namespace, pod.Name, k8s.FRRContainerName)

		nodeIP := nodeInternalIP(node)
		env := buildTargetEnv(cfg, nodeIP, i+1, false)
		if err := runTargetSetup(podExec, env); err != nil {
			return fmt.Errorf("node setup on %s: %w", node.Name, err)
		}
	}

	extIndex := len(nodes) + 1
	extEnv := buildTargetEnv(cfg, externalFRR.Ipv4, extIndex, false)
	extExec := executor.ForContainer(externalFRR.Name)
	if err := runTargetSetup(extExec, extEnv); err != nil {
		return fmt.Errorf("node setup on external FRR %s: %w", externalFRR.Name, err)
	}

	if err := configureExternalFRR(extExec, nodes, externalFRR, cfg); err != nil {
		return fmt.Errorf("configuring external FRR: %w", err)
	}

	time.Sleep(5 * time.Second)
	return nil
}

// CleanupEVPN tears down EVPN networking on cluster nodes and the external FRR
// container.
func CleanupEVPN(cs clientset.Interface, nodes []corev1.Node, externalFRR *frrcontainer.FRR, cfg EVPNConfig) error {
	extExec := executor.ForContainer(externalFRR.Name)
	cleanupExternalFRR(extExec, externalFRR, cfg)

	extIndex := len(nodes) + 1
	extEnv := buildTargetEnv(cfg, externalFRR.Ipv4, extIndex, true)
	if err := runTargetSetup(extExec, extEnv); err != nil {
		return fmt.Errorf("cleanup on external FRR %s: %w", externalFRR.Name, err)
	}

	pods, err := k8s.FRRK8sPods(cs)
	if err != nil {
		return fmt.Errorf("listing frr-k8s pods: %w", err)
	}

	for i, node := range nodes {
		pod, err := podForNode(pods, node.Name)
		if err != nil {
			return err
		}
		podExec := executor.ForPod(pod.Namespace, pod.Name, k8s.FRRContainerName)
		env := buildTargetEnv(cfg, nodeInternalIP(node), i+1, true)
		if err := runTargetSetup(podExec, env); err != nil {
			return fmt.Errorf("cleanup on %s: %w", node.Name, err)
		}
	}

	return nil
}

func podForNode(pods []*corev1.Pod, nodeName string) (*corev1.Pod, error) {
	for _, pod := range pods {
		if pod.Spec.NodeName == nodeName {
			return pod, nil
		}
	}
	return nil, fmt.Errorf("no frr-k8s pod found on node %s", nodeName)
}

func nodeInternalIP(node corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP && !strings.Contains(addr.Address, ":") {
			return addr.Address
		}
	}
	return ""
}

func buildTargetEnv(cfg EVPNConfig, vtepIP string, targetIndex int, cleanup bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "export EVPN_VTEP_IP=%s\n", vtepIP)
	fmt.Fprintf(&b, "export EVPN_BRIDGE=%s\n", cfg.Bridge)
	fmt.Fprintf(&b, "export EVPN_VXLAN=%s\n", cfg.Vxlan)

	if cfg.L2VNI > 0 {
		fmt.Fprintf(&b, "export EVPN_L2_VNI=%d\n", cfg.L2VNI)
		fmt.Fprintf(&b, "export EVPN_L2_VLAN_ID=%d\n", cfg.L2VLANID)
		fmt.Fprintf(&b, "export EVPN_L2_IP=%s\n", fmt.Sprintf(cfg.L2IPFmt, targetIndex))
	}

	if cfg.L3VNI > 0 {
		fmt.Fprintf(&b, "export EVPN_L3_VNI=%d\n", cfg.L3VNI)
		fmt.Fprintf(&b, "export EVPN_L3_VLAN_ID=%d\n", cfg.L3VLANID)
		fmt.Fprintf(&b, "export EVPN_L3_VRF=%s\n", cfg.L3VRF)
		fmt.Fprintf(&b, "export EVPN_L3_VRF_TABLE=%d\n", cfg.L3VRFTable)
		fmt.Fprintf(&b, "export EVPN_L3_PREFIX=%s\n", fmt.Sprintf(cfg.L3PrefixFmt, targetIndex))
	}

	if cleanup {
		fmt.Fprintf(&b, "export EVPN_CLEANUP=true\n")
	}

	return b.String()
}

func runTargetSetup(exec executor.Executor, env string) error {
	script := env + "\n" + evpnNodeSetupScript
	out, err := exec.Exec("bash", "-c", script)
	if err != nil {
		return fmt.Errorf("running script: %w\noutput: %s", err, out)
	}
	return nil
}

func configureExternalFRR(exec executor.Executor, nodes []corev1.Node, externalFRR *frrcontainer.FRR, cfg EVPNConfig) error {
	extASN := externalFRR.RouterConfig.ASN

	var nodeIPs []string
	for _, node := range nodes {
		nodeIPs = append(nodeIPs, nodeInternalIP(node))
	}

	// Phase 1: VRF-VNI binding (zebra config)
	if cfg.L3VNI > 0 {
		cmds := []string{
			"configure terminal",
			fmt.Sprintf("vrf %s", cfg.L3VRF),
			fmt.Sprintf("vni %d", cfg.L3VNI),
			"exit-vrf",
			"end",
		}
		if err := vtysh(exec, cmds); err != nil {
			return fmt.Errorf("phase 1 VRF-VNI binding: %w", err)
		}
		time.Sleep(2 * time.Second)
	}

	// Phase 2: Global EVPN BGP config
	cmds := []string{
		"configure terminal",
		fmt.Sprintf("router bgp %d", extASN),
	}

	for _, ip := range nodeIPs {
		cmds = append(cmds, fmt.Sprintf("neighbor %s remote-as %d", ip, FRRK8sASN))
	}

	cmds = append(cmds, "address-family l2vpn evpn")
	for _, ip := range nodeIPs {
		cmds = append(cmds, fmt.Sprintf("neighbor %s activate", ip))
	}
	cmds = append(cmds, "advertise-all-vni")

	if cfg.L2VNI > 0 {
		cmds = append(cmds,
			fmt.Sprintf("vni %d", cfg.L2VNI),
			fmt.Sprintf("rd %d:%d", extASN, cfg.L2VNI),
			fmt.Sprintf("route-target import %d:%d", FRRK8sASN, cfg.L2VNI),
			fmt.Sprintf("route-target import %d:%d", extASN, cfg.L2VNI),
			fmt.Sprintf("route-target export %d:%d", extASN, cfg.L2VNI),
			"exit-vni",
		)
	}

	cmds = append(cmds, "exit-address-family", "end")

	if err := vtysh(exec, cmds); err != nil {
		return fmt.Errorf("phase 2 global EVPN BGP: %w", err)
	}

	time.Sleep(2 * time.Second)

	// Phase 3: IP-VRF BGP config (L3 VNI)
	if cfg.L3VNI > 0 {
		cmds = []string{
			"configure terminal",
			fmt.Sprintf("router bgp %d vrf %s", extASN, cfg.L3VRF),
			"address-family ipv4 unicast",
			"redistribute connected",
			"exit-address-family",
			"address-family l2vpn evpn",
			fmt.Sprintf("rd %d:%d", extASN, cfg.L3VNI),
			fmt.Sprintf("route-target import %d:%d", FRRK8sASN, cfg.L3VNI),
			fmt.Sprintf("route-target import %d:%d", extASN, cfg.L3VNI),
			fmt.Sprintf("route-target export %d:%d", extASN, cfg.L3VNI),
			"advertise ipv4 unicast",
			"advertise ipv6 unicast",
			"exit-address-family",
			"end",
		}
		if err := vtysh(exec, cmds); err != nil {
			return fmt.Errorf("phase 3 IP-VRF BGP: %w", err)
		}
	}

	return nil
}

func cleanupExternalFRR(exec executor.Executor, externalFRR *frrcontainer.FRR, cfg EVPNConfig) {
	extASN := externalFRR.RouterConfig.ASN

	if cfg.L3VNI > 0 {
		vtysh(exec, []string{ //nolint:errcheck
			"configure terminal",
			fmt.Sprintf("vrf %s", cfg.L3VRF),
			fmt.Sprintf("no vni %d", cfg.L3VNI),
			"exit-vrf",
			"end",
		})
	}

	if cfg.L2VNI > 0 {
		vtysh(exec, []string{ //nolint:errcheck
			"configure terminal",
			fmt.Sprintf("router bgp %d", extASN),
			"address-family l2vpn evpn",
			fmt.Sprintf("no vni %d", cfg.L2VNI),
			"exit-address-family",
			"end",
		})
	}

	if cfg.L3VNI > 0 {
		vtysh(exec, []string{ //nolint:errcheck
			"configure terminal",
			fmt.Sprintf("no router bgp %d vrf %s", extASN, cfg.L3VRF),
			"end",
		})

		vtysh(exec, []string{ //nolint:errcheck
			"configure terminal",
			fmt.Sprintf("no vrf %s", cfg.L3VRF),
			"end",
		})
	}
}

func vtysh(exec executor.Executor, cmds []string) error {
	args := make([]string, 0, len(cmds)*2)
	for _, cmd := range cmds {
		args = append(args, "-c", cmd)
	}
	out, err := exec.Exec("vtysh", args...)
	if err != nil {
		return fmt.Errorf("vtysh failed: %w\noutput: %s", err, out)
	}
	return nil
}
