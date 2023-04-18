package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

type ServiceInfo struct {
	Name string
	Ip   string
}

type Service struct {
	ServiceInfo ServiceInfo
	stop        chan error
	leaseId     clientv3.LeaseID
	client      *clientv3.Client
}

func NewService(serviceInfo ServiceInfo, endpoints []string) (service *Service, err error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 10,
	})
	if err != nil {
		return nil, err
	}

	service = &Service{
		ServiceInfo: serviceInfo,
		client:      client,
	}
	return
}

func (s *Service) Start(ctx context.Context) (err error) {
	alive, err := s.KeepAlive(ctx)
	if err != nil {
		return
	}

	for {
		select {
		case err = <-s.stop: // 服务关闭返回错误
			return err
		case <-s.client.Ctx().Done(): // etcd关闭
			return errors.New("server closed")
		case _, ok := <-alive:
			if !ok {
				return s.revoke(ctx)
			}
		}
	}
}

func (s *Service) KeepAlive(ctx context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	info := s.ServiceInfo
	key := s.getKey()
	val, _ := json.Marshal(info)

	// 创建租约
	leaseResp, err := s.client.Grant(ctx, 5)
	if err != nil {
		return nil, err
	}

	// 写入etcd
	_, err = s.client.Put(ctx, key, string(val), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return nil, err
	}

	s.leaseId = leaseResp.ID
	return s.client.KeepAlive(ctx, leaseResp.ID)
}

// 取消租约
func (s *Service) revoke(ctx context.Context) error {
	_, err := s.client.Revoke(ctx, s.leaseId)
	return err
}

func (s *Service) getKey() string {
	return s.ServiceInfo.Name + "/" + s.ServiceInfo.Ip
}

type Discovery struct {
	endpoints  []string
	service    string
	client     *clientv3.Client
	clientConn resolver.ClientConn
}

func NewDiscovery(endpoints []string, service string) resolver.Builder {
	return &Discovery{
		endpoints: endpoints,
		service:   service,
	}
}

func (d *Discovery) ResolveNow(rn resolver.ResolveNowOptions) {}

func (d *Discovery) Close() {}

func (d *Discovery) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	var err error
	d.client, err = clientv3.New(clientv3.Config{
		Endpoints: d.endpoints,
	})
	if err != nil {
		return nil, err
	}

	d.clientConn = cc

	go d.watch(d.service)

	return d, nil
}

func (d *Discovery) Scheme() string {
	return "etcd"
}

func (d *Discovery) watch(service string) {
	addrM := make(map[string]resolver.Address)
	state := resolver.State{}

	update := func() {
		addrList := make([]resolver.Address, 0, len(addrM))
		for _, address := range addrM {
			addrList = append(addrList, address)
		}
		state.Addresses = addrList
		err := d.clientConn.UpdateState(state)
		if err != nil {
			fmt.Println("更新地址出错：", err)
		}
	}
	resp, err := d.client.Get(context.Background(), service, clientv3.WithPrefix())
	if err != nil {
		fmt.Println("获取地址出错：", err)
	} else {
		for i, kv := range resp.Kvs {
			info := &ServiceInfo{}
			err = json.Unmarshal(kv.Value, info)
			if err != nil {
				fmt.Println("解析value失败：", err)
			}
			addrM[string(resp.Kvs[i].Key)] = resolver.Address{
				Addr:       info.Ip,
				ServerName: info.Name,
			}
		}
	}

	update()

	dch := d.client.Watch(context.Background(), service, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for response := range dch {
		for _, event := range response.Events {
			switch event.Type {
			case mvccpb.PUT:
				info := &ServiceInfo{}
				err = json.Unmarshal(event.Kv.Value, info)
				if err != nil {
					fmt.Println("监听时解析value报错：", err)
				} else {
					addrM[string(event.Kv.Key)] = resolver.Address{Addr: info.Ip}
				}
				fmt.Println(string(event.Kv.Key))
			case mvccpb.DELETE:
				delete(addrM, string(event.Kv.Key))
				fmt.Println(string(event.Kv.Key))
			}
		}
		update()
	}
}
