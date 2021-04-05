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

package subnet

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/flannel-io/flannel/pkg/ip"
	"golang.org/x/net/context"
)

var (
	ErrLeaseTaken  = errors.New("subnet: lease already taken")
	ErrNoMoreTries = errors.New("subnet: no more tries")
	subnetRegex    = regexp.MustCompile(`(\d+\.\d+.\d+.\d+)-(\d+)`)
)

type LeaseAttrs struct {
	// 与外部主机交互的网卡IP，如果不配置会取默认路由的网卡IP地址
	PublicIP    ip.IP4
	// backend类型，host-gw, udp, vxlan等
	BackendType string          `json:",omitempty"`
	// 配置信息，不同后端配置不同
	BackendData json.RawMessage `json:",omitempty"`
}

type Lease struct {
	// 子网信息
	Subnet     ip.IP4Net
	// flanneld进程所在机器信息
	Attrs      LeaseAttrs
	// 租赁到期时间
	Expiration time.Time

	Asof uint64
}

func (l *Lease) Key() string {
	return MakeSubnetKey(l.Subnet)
}

type (
	EventType int

	Event struct {
		Type  EventType `json:"type"`
		Lease Lease     `json:"lease,omitempty"`
	}
)

const (
	EventAdded EventType = iota
	EventRemoved
)

type LeaseWatchResult struct {
	// Either Events or Snapshot will be set.  If Events is empty, it means
	// the cursor was out of range and Snapshot contains the current list
	// of items, even if empty.
	Events   []Event     `json:"events"`
	Snapshot []Lease     `json:"snapshot"`
	Cursor   interface{} `json:"cursor"`
}

func (et EventType) MarshalJSON() ([]byte, error) {
	s := ""

	switch et {
	case EventAdded:
		s = "added"
	case EventRemoved:
		s = "removed"
	default:
		return nil, errors.New("bad event type")
	}
	return json.Marshal(s)
}

func (et *EventType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "\"added\"":
		*et = EventAdded
	case "\"removed\"":
		*et = EventRemoved
	default:
		fmt.Println(string(data))
		return errors.New("bad event type")
	}

	return nil
}

func ParseSubnetKey(s string) *ip.IP4Net {
	if parts := subnetRegex.FindStringSubmatch(s); len(parts) == 3 {
		snIp := net.ParseIP(parts[1]).To4()
		prefixLen, err := strconv.ParseUint(parts[2], 10, 5)
		if snIp != nil && err == nil {
			return &ip.IP4Net{IP: ip.FromIP(snIp), PrefixLen: uint(prefixLen)}
		}
	}

	return nil
}

func MakeSubnetKey(sn ip.IP4Net) string {
	return sn.StringSep(".", "-")
}
// https://zhuanlan.zhihu.com/p/277486806
type Manager interface {
	// 获取存储在etcd的网络配置
	// 初始化时需要获取网络配置，包括子网信息，backendType等
	GetNetworkConfig(ctx context.Context) (*Config, error)
	// 子网租赁，类似dhcp
	AcquireLease(ctx context.Context, attrs *LeaseAttrs) (*Lease, error)
	// 子网续租，类似dhcp
	// 定期更新子网过期时间，保证subnet在运行期间不过期
	RenewLease(ctx context.Context, lease *Lease) error
	// 子网变化监控，
	// 主要是backend监控到子网变化时操作路由
	WatchLease(ctx context.Context, sn ip.IP4Net, cursor interface{}) (LeaseWatchResult, error)
	WatchLeases(ctx context.Context, cursor interface{}) (LeaseWatchResult, error)

	Name() string
}
