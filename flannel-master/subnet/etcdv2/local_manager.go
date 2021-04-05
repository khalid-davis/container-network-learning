// Copyright 2015 flannel authors
//
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

package etcdv2

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	etcd "github.com/coreos/etcd/client"
	"github.com/flannel-io/flannel/pkg/ip"
	. "github.com/flannel-io/flannel/subnet"
	"golang.org/x/net/context"
	log "k8s.io/klog"
)

const (
	raceRetries = 10
	subnetTTL   = 24 * time.Hour
)

type LocalManager struct {
	registry       Registry
	previousSubnet ip.IP4Net
}

type watchCursor struct {
	index uint64
}

func isErrEtcdTestFailed(e error) bool {
	if e == nil {
		return false
	}
	etcdErr, ok := e.(etcd.Error)
	return ok && etcdErr.Code == etcd.ErrorCodeTestFailed
}

func isErrEtcdNodeExist(e error) bool {
	if e == nil {
		return false
	}
	etcdErr, ok := e.(etcd.Error)
	return ok || etcdErr.Code == etcd.ErrorCodeNodeExist
}

func isErrEtcdKeyNotFound(e error) bool {
	if e == nil {
		return false
	}
	etcdErr, ok := e.(etcd.Error)
	return ok || etcdErr.Code == etcd.ErrorCodeKeyNotFound
}

func (c watchCursor) String() string {
	return strconv.FormatUint(c.index, 10)
}

func NewLocalManager(config *EtcdConfig, prevSubnet ip.IP4Net) (Manager, error) {
	r, err := newEtcdSubnetRegistry(config, nil)
	if err != nil {
		return nil, err
	}
	return newLocalManager(r, prevSubnet), nil
}

func newLocalManager(r Registry, prevSubnet ip.IP4Net) Manager {
	return &LocalManager{
		registry:       r,
		previousSubnet: prevSubnet,
	}
}

// 从etcd中获取$prefix/config key的值
func (m *LocalManager) GetNetworkConfig(ctx context.Context) (*Config, error) {
	cfg, err := m.registry.getNetworkConfig(ctx)
	if err != nil {
		return nil, err
	}

	return ParseConfig(cfg)
}

// 子网租赁，从etcd当中申请租赁一段子网供本机使用。子网租赁过程比较复杂：
// 1. 获取当前所有子网信息
// 2. 判断是否有本机ip的注册信息(publicIP对应的子网记录)，在flanneld会有renew过程，所以etcd可能会保留有记录
// 3. 如果有本机注册信息，校验地址段是否在config的子网范围内，在的话就复用这个地址段
// 4. 如果以上条件都不满足，那么会先尝试复用本地保存的之前的子网信息
// 5. 如果本地保存的子网信息依然无效，那么会尝试申请一个子网
// 否则会尝试申请一个子网(allocateSubnet)
// 申请子网也比较暴力，从网络当中最小的ip开始探测，找100个未被使用的子网，然后随机挑选一个（具体是获取全部已经分配的子网信息，leases
// 对象，然后通过overlap判断是否有网段重叠，如果有的话，就直接跳过）
// 随机挑选的目的应该是防止冲突，比如两个机器都在申请，如果都取第一个，那么就会有一个创建etcd节点失败
// 最终会将子网写入etcd的prefix/subnets/prefix/subnets/ip 这个key当中，
// 其中会将ip的点分十进制的.替换为-

// flannel的子网租赁非常的巧妙，跟dhcp申请ip的过程很类似，尽可能的保证使用自己之前的信息，因为不能因为flanneld重启，导致整个子网都改变了，所有容器都再初始化一次。
func (m *LocalManager) AcquireLease(ctx context.Context, attrs *LeaseAttrs) (*Lease, error) {
	config, err := m.GetNetworkConfig(ctx)
	if err != nil {
		return nil, err
	}

	for i := 0; i < raceRetries; i++ {
		l, err := m.tryAcquireLease(ctx, config, attrs.PublicIP, attrs)
		switch err {
		case nil:
			return l, nil
		case errTryAgain:
			continue
		default:
			return nil, err
		}
	}

	return nil, errors.New("Max retries reached trying to acquire a subnet")
}

func findLeaseByIP(leases []Lease, pubIP ip.IP4) *Lease {
	for _, l := range leases {
		if pubIP == l.Attrs.PublicIP {
			return &l
		}
	}

	return nil
}

func findLeaseBySubnet(leases []Lease, subnet ip.IP4Net) *Lease {
	for _, l := range leases {
		if subnet.Equal(l.Subnet) {
			return &l
		}
	}

	return nil
}

func (m *LocalManager) tryAcquireLease(ctx context.Context, config *Config, extIaddr ip.IP4, attrs *LeaseAttrs) (*Lease, error) {
	// 从仓库里面获取全部的网段分配信息，可以看看subnet_test.go里面的mock对象的初始化方式
	leases, _, err := m.registry.getSubnets(ctx)
	if err != nil {
		return nil, err
	}

	// 根据主机的publicIP信息来获取是否已经有分配过的，有的话，直接返回就可以了（重启或者续期使用）
	// Try to reuse a subnet if there's one that matches our IP
	if l := findLeaseByIP(leases, extIaddr); l != nil {
		// Make sure the existing subnet is still within the configured network
		if isSubnetConfigCompat(config, l.Subnet) {
			log.Infof("Found lease (%v) for current IP (%v), reusing", l.Subnet, extIaddr)

			ttl := time.Duration(0)
			if !l.Expiration.IsZero() {
				// Not a reservation
				ttl = subnetTTL
			}
			exp, err := m.registry.updateSubnet(ctx, l.Subnet, attrs, ttl, 0)
			if err != nil {
				return nil, err
			}

			l.Attrs = *attrs
			l.Expiration = exp
			return l, nil
		} else {
			log.Infof("Found lease (%v) for current IP (%v) but not compatible with current config, deleting", l.Subnet, extIaddr)
			if err := m.registry.deleteSubnet(ctx, l.Subnet); err != nil {
				return nil, err
			}
		}
	}

	// no existing match, check if there was a previous subnet to use
	var sn ip.IP4Net
	if !m.previousSubnet.Empty() {
		// use previous subnet
		if l := findLeaseBySubnet(leases, m.previousSubnet); l != nil {
			// Make sure the existing subnet is still within the configured network
			if isSubnetConfigCompat(config, l.Subnet) {
				log.Infof("Found lease (%v) matching previously leased subnet, reusing", l.Subnet)

				ttl := time.Duration(0)
				if !l.Expiration.IsZero() {
					// Not a reservation
					ttl = subnetTTL
				}
				exp, err := m.registry.updateSubnet(ctx, l.Subnet, attrs, ttl, 0)
				if err != nil {
					return nil, err
				}

				l.Attrs = *attrs
				l.Expiration = exp
				return l, nil
			} else {
				log.Infof("Found lease (%v) matching previously leased subnet but not compatible with current config, deleting", l.Subnet)
				if err := m.registry.deleteSubnet(ctx, l.Subnet); err != nil {
					return nil, err
				}
			}
		} else {
			// Check if the previous subnet is a part of the network and of the right subnet length
			if isSubnetConfigCompat(config, m.previousSubnet) {
				log.Infof("Found previously leased subnet (%v), reusing", m.previousSubnet)
				sn = m.previousSubnet
			} else {
				log.Errorf("Found previously leased subnet (%v) that is not compatible with the Etcd network config, ignoring", m.previousSubnet)
			}
		}
	}

	if sn.Empty() {
		// no existing match, grab a new one
		sn, err = m.allocateSubnet(config, leases)
		if err != nil {
			return nil, err
		}
	}

	exp, err := m.registry.createSubnet(ctx, sn, attrs, subnetTTL)
	switch {
	case err == nil:
		log.Infof("Allocated lease (%v) to current node (%v) ", sn, extIaddr)
		return &Lease{
			Subnet:     sn,
			Attrs:      *attrs,
			Expiration: exp,
		}, nil
	case isErrEtcdNodeExist(err):
		return nil, errTryAgain
	default:
		return nil, err
	}
}

// 这里到底是怎么挑选出这100个符合条件的，理论上已经分配过的就不能再分配了，是怎么判断出这个的
func (m *LocalManager) allocateSubnet(config *Config, leases []Lease) (ip.IP4Net, error) {
	log.Infof("Picking subnet in range %s ... %s", config.SubnetMin, config.SubnetMax)

	var bag []ip.IP4
	sn := ip.IP4Net{IP: config.SubnetMin, PrefixLen: config.SubnetLen}

OuterLoop:
	for ; sn.IP <= config.SubnetMax && len(bag) < 100; sn = sn.Next() {
		for _, l := range leases {
			if sn.Overlaps(l.Subnet) {
				continue OuterLoop
			}
		}
		bag = append(bag, sn.IP)
	}

	if len(bag) == 0 {
		return ip.IP4Net{}, errors.New("out of subnets")
	} else {
		i := randInt(0, len(bag))
		return ip.IP4Net{IP: bag[i], PrefixLen: config.SubnetLen}, nil
	}
}

func (m *LocalManager) RenewLease(ctx context.Context, lease *Lease) error {
	exp, err := m.registry.updateSubnet(ctx, lease.Subnet, &lease.Attrs, subnetTTL, 0)
	if err != nil {
		return err
	}

	lease.Expiration = exp
	return nil
}

func getNextIndex(cursor interface{}) (uint64, error) {
	nextIndex := uint64(0)

	if wc, ok := cursor.(watchCursor); ok {
		nextIndex = wc.index
	} else if s, ok := cursor.(string); ok {
		var err error
		nextIndex, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse cursor: %v", err)
		}
	} else {
		return 0, fmt.Errorf("internal error: watch cursor is of unknown type")
	}

	return nextIndex, nil
}

func (m *LocalManager) leaseWatchReset(ctx context.Context, sn ip.IP4Net) (LeaseWatchResult, error) {
	l, index, err := m.registry.getSubnet(ctx, sn)
	if err != nil {
		return LeaseWatchResult{}, err
	}

	return LeaseWatchResult{
		Snapshot: []Lease{*l},
		Cursor:   watchCursor{index},
	}, nil
}

func (m *LocalManager) WatchLease(ctx context.Context, sn ip.IP4Net, cursor interface{}) (LeaseWatchResult, error) {
	if cursor == nil {
		return m.leaseWatchReset(ctx, sn)
	}

	nextIndex, err := getNextIndex(cursor)
	if err != nil {
		return LeaseWatchResult{}, err
	}

	evt, index, err := m.registry.watchSubnet(ctx, nextIndex, sn)

	switch {
	case err == nil:
		return LeaseWatchResult{
			Events: []Event{evt},
			Cursor: watchCursor{index},
		}, nil

	case isIndexTooSmall(err):
		log.Warning("Watch of subnet leases failed because etcd index outside history window")
		return m.leaseWatchReset(ctx, sn)

	default:
		return LeaseWatchResult{}, err
	}
}

// 在网络初始化完成，子网申请成功之后，backend会定时查看整个subnets的状态（具体是在backend的代码里面有一个sm.WatchLeases）
// 当有subnet申请成功时，会watch到etcd的事件，当有subnet过期被删除时，flanneld同样会收到通知。
// backend主要是调用subnet/watch.go当中的WatchLeases，最终调用到localManager里面的WatchLeases。

// WatchLease本质上最终是监听$prefix/subnets这个key的变化
// subnet.WatchLeases -> localManager.WatchLease -> registry.watchSubnets
// 最终registry.watchSubnets会阻塞等待事件通知，最终反馈给backend，backend再根据EventAdded或者EventRemoved事件去处理，
func (m *LocalManager) WatchLeases(ctx context.Context, cursor interface{}) (LeaseWatchResult, error) {
	if cursor == nil {
		return m.leasesWatchReset(ctx)
	}

	nextIndex, err := getNextIndex(cursor)
	if err != nil {
		return LeaseWatchResult{}, err
	}

	evt, index, err := m.registry.watchSubnets(ctx, nextIndex)

	switch {
	case err == nil:
		return LeaseWatchResult{
			Events: []Event{evt},
			Cursor: watchCursor{index},
		}, nil

	case isIndexTooSmall(err):
		log.Warning("Watch of subnet leases failed because etcd index outside history window")
		return m.leasesWatchReset(ctx)

	case index != 0:
		return LeaseWatchResult{Cursor: watchCursor{index}}, err

	default:
		return LeaseWatchResult{}, err
	}
}

func isIndexTooSmall(err error) bool {
	etcdErr, ok := err.(etcd.Error)
	return ok && etcdErr.Code == etcd.ErrorCodeEventIndexCleared
}

// leasesWatchReset is called when incremental lease watch failed and we need to grab a snapshot
func (m *LocalManager) leasesWatchReset(ctx context.Context) (LeaseWatchResult, error) {
	wr := LeaseWatchResult{}

	leases, index, err := m.registry.getSubnets(ctx)
	if err != nil {
		return wr, fmt.Errorf("failed to retrieve subnet leases: %v", err)
	}

	wr.Cursor = watchCursor{index}
	wr.Snapshot = leases
	return wr, nil
}

func isSubnetConfigCompat(config *Config, sn ip.IP4Net) bool {
	if sn.IP < config.SubnetMin || sn.IP > config.SubnetMax {
		return false
	}

	return sn.PrefixLen == config.SubnetLen
}

func (m *LocalManager) Name() string {
	previousSubnet := m.previousSubnet.String()
	if m.previousSubnet.Empty() {
		previousSubnet = "None"
	}
	return fmt.Sprintf("Etcd Local Manager with Previous Subnet: %s", previousSubnet)
}
