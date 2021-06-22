/*
 * Copyright 2019 SAP SE or an SAP affiliate company. All rights reserved. h file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *
 */

package provider

import (
	"context"
	"sync"
	"time"

	"github.com/gardener/controller-manager-library/pkg/logger"
	"github.com/gardener/external-dns-management/pkg/dns/provider/errors"
	"github.com/gardener/external-dns-management/pkg/server/metrics"
)

type StateTTLGetter func(zoneid string) time.Duration

type ZoneCacheConfig struct {
	context               context.Context
	logger                logger.LogContext
	zoneID                QualifiedZoneID
	zonesTTL              time.Duration
	stateTTLGetter        StateTTLGetter
	disableZoneStateCache bool
}

func NewTestZoneCacheConfig(stateTTL time.Duration) *ZoneCacheConfig {
	return &ZoneCacheConfig{
		stateTTLGetter: func(string) time.Duration { return stateTTL },
	}
}

func (c *ZoneCacheConfig) CopyWithDisabledZoneStateCache() *ZoneCacheConfig {
	return &ZoneCacheConfig{context: c.context, logger: c.logger,
		zoneID:   c.zoneID,
		zonesTTL: c.zonesTTL, stateTTLGetter: c.stateTTLGetter, disableZoneStateCache: true}
}

type ZoneCacheZoneUpdater func(cache ZoneCache) (DNSHostedZones, error)

type ZoneCacheStateUpdater func(zone DNSHostedZone, cache ZoneCache) (DNSZoneState, error)

type ZoneCache interface {
	GetZones() (DNSHostedZones, error)
	GetZoneState(zone DNSHostedZone) (DNSZoneState, error)
	ApplyRequests(logctx logger.LogContext, err error, zone DNSHostedZone, reqs []*ChangeRequest)
	GetForwardedDomains(zoneid string) []string
	SetForwardedDomains(zoneid string, value []string)
	Release()
	ReportZoneStateConflict(zone DNSHostedZone, err error) bool
}

func NewZoneCache(config ZoneCacheConfig, metrics Metrics, cacheForwardedDomains bool,
	zonesUpdater ZoneCacheZoneUpdater, stateUpdater ZoneCacheStateUpdater) (ZoneCache, error) {
	var zonesForwardedDomains *zonesForwardedDomains
	if cacheForwardedDomains {
		zonesForwardedDomains = newZonesForwardedDomains()
	}
	common := abstractZonesCache{config: config, zonesUpdater: zonesUpdater, stateUpdater: stateUpdater, zonesForwardedDomains: zonesForwardedDomains}
	if config.disableZoneStateCache {
		cache := &onlyZonesCache{abstractZonesCache: common}
		return cache, nil
	} else {
		return newDefaultZoneCache(common, metrics)
	}
}

type zonesForwardedDomains struct {
	lock             sync.Mutex
	forwardedDomains map[string][]string
}

func newZonesForwardedDomains() *zonesForwardedDomains {
	return &zonesForwardedDomains{forwardedDomains: map[string][]string{}}
}

func (fd *zonesForwardedDomains) GetForwardedDomains(zoneid string) []string {
	fd.lock.Lock()
	defer fd.lock.Unlock()
	return fd.forwardedDomains[zoneid]
}

func (fd *zonesForwardedDomains) SetForwardedDomains(zoneid string, value []string) {
	fd.lock.Lock()
	defer fd.lock.Unlock()

	if value != nil {
		fd.forwardedDomains[zoneid] = value
	} else {
		delete(fd.forwardedDomains, zoneid)
	}
}

func (fd *zonesForwardedDomains) ZoneDeleted(zoneid string) {
	fd.lock.Lock()
	defer fd.lock.Unlock()
	delete(fd.forwardedDomains, zoneid)
}

type abstractZonesCache struct {
	config                ZoneCacheConfig
	zones                 DNSHostedZones
	zonesErr              error
	zonesNext             time.Time
	zonesUpdater          ZoneCacheZoneUpdater
	stateUpdater          ZoneCacheStateUpdater
	zonesForwardedDomains *zonesForwardedDomains
}

func (c *abstractZonesCache) GetForwardedDomains(zoneid string) []string {
	return c.zonesForwardedDomains.GetForwardedDomains(zoneid)
}

func (c *abstractZonesCache) SetForwardedDomains(zoneid string, forwarded []string) {
	c.zonesForwardedDomains.SetForwardedDomains(zoneid, forwarded)
}

type onlyZonesCache struct {
	abstractZonesCache
	lock sync.Mutex
}

var _ ZoneCache = &onlyZonesCache{}

func (c *onlyZonesCache) GetZones() (DNSHostedZones, error) {
	zones, err := c.zonesUpdater(c)
	return zones, err
}

func (c *onlyZonesCache) GetZoneState(zone DNSHostedZone) (DNSZoneState, error) {
	state, err := c.stateUpdater(zone, c)
	return state, err
}

func (c *onlyZonesCache) ApplyRequests(logctx logger.LogContext, err error, zone DNSHostedZone, reqs []*ChangeRequest) {
}

func (c *onlyZonesCache) ReportZoneStateConflict(zone DNSHostedZone, err error) bool {
	return false
}

func (c *onlyZonesCache) Release() {
}

type defaultZoneCache struct {
	abstractZonesCache
	lock    sync.Mutex
	logger  logger.LogContext
	metrics Metrics
	state   *zoneState

	backoffOnError time.Duration
}

var _ ZoneCache = &defaultZoneCache{}

func newDefaultZoneCache(common abstractZonesCache, metrics Metrics) (*defaultZoneCache, error) {
	state := &zoneState{
		inMemory:       NewInMemory(),
		stateTTLGetter: common.config.stateTTLGetter,
		next:           map[string]updateTimestamp{},
	}
	cache := &defaultZoneCache{abstractZonesCache: common, logger: common.config.logger, metrics: metrics, state: state}
	return cache, nil
}

func (c *defaultZoneCache) GetZones() (DNSHostedZones, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if time.Now().After(c.zonesNext) {
		c.zones, c.zonesErr = c.zonesUpdater(c)
		updateTime := time.Now()
		if c.zonesErr != nil {
			// if getzones fails, don't wait zonesTTL, but use an exponential backoff
			// to recover fast from temporary failures like throttling, network problems...
			backoff := c.nextBackoff()
			c.zonesNext = updateTime.Add(backoff)
		} else {
			c.clearBackoff()
			c.zonesNext = updateTime.Add(c.config.zonesTTL)
		}
		c.state.RestrictCacheToZones(c.zones)
	} else {
		c.metrics.AddGenericRequests(M_CACHED_GETZONES, 1)
	}
	return c.zones, c.zonesErr
}

func (c *defaultZoneCache) nextBackoff() time.Duration {
	next := c.backoffOnError*5/4 + 2*time.Second
	maxBackoff := c.config.zonesTTL / 4
	if next > maxBackoff {
		next = maxBackoff
	}
	c.backoffOnError = next
	return next
}

func (c *defaultZoneCache) clearBackoff() {
	c.backoffOnError = 0
}

func (c *defaultZoneCache) GetZoneState(zone DNSHostedZone) (DNSZoneState, error) {
	state, cached, err := c.state.GetZoneState(zone, c)
	if cached {
		c.metrics.AddZoneRequests(zone.Id(), M_CACHED_GETZONESTATE, 1)
	}
	return state, err
}

func (c *defaultZoneCache) ReportZoneStateConflict(zone DNSHostedZone, err error) bool {
	return c.state.ReportZoneStateConflict(zone, err)
}

func (c *defaultZoneCache) deleteZoneState(zone DNSHostedZone) {
	c.state.DeleteZoneState(zone)
}

func (c *defaultZoneCache) ApplyRequests(logctx logger.LogContext, err error, zone DNSHostedZone, reqs []*ChangeRequest) {
	if err == nil {
		c.state.ExecuteRequests(zone, reqs)
	} else {
		if !errors.IsThrottlingError(err) {
			logctx.Infof("zone cache discarded because of error during ExecuteRequests")
			c.deleteZoneState(zone)
			metrics.AddZoneCacheDiscarding(zone.ProviderType(), zone.Id())
		} else {
			logctx.Infof("zone cache untouched (only throttling during ExecuteRequests)")
		}
	}
}

func (c *defaultZoneCache) Release() {
	c.state.RestrictCacheToZones(DNSHostedZones{})
}

type updateTimestamp struct {
	updateStart time.Time
	updateEnd   time.Time
}

type zoneState struct {
	lock           sync.Mutex
	stateTTLGetter StateTTLGetter
	inMemory       *InMemory
	next           map[string]updateTimestamp
}

func (s *zoneState) GetZoneState(zone DNSHostedZone, cache *defaultZoneCache) (DNSZoneState, bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	next, ok := s.next[zone.Id()]
	start := time.Now()
	ttl := s.stateTTLGetter(zone.Id())
	if !ok || start.After(next.updateEnd.Add(ttl)) {
		state, err := cache.stateUpdater(zone, cache)
		if err == nil {
			s.next[zone.Id()] = updateTimestamp{start, time.Now()}
			s.inMemory.SetZone(zone, state)
		} else {
			s.deleteZoneState(zone)
		}
		return state, false, err
	}

	state, _ := s.inMemory.CloneZoneState(zone)
	return state, true, nil
}

func (s *zoneState) ReportZoneStateConflict(zone DNSHostedZone, err error) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	next, found := s.next[zone.Id()]
	if found {
		ownerConflict, ok := err.(*errors.AlreadyBusyForOwner)
		if ok {
			if ownerConflict.EntryCreatedAt.After(next.updateStart) {
				// If a DNSEntry ownership is moved to another DNS controller manager (e.g. shoot recreation on another seed)
				// the zone cache may have stale owner information. In this case the cache is invalidated
				// if the entry is newer than the last cache refresh.
				s.deleteZoneState(zone)
				return true
			}
		}
	}
	return false
}

func (s *zoneState) ExecuteRequests(zone DNSHostedZone, reqs []*ChangeRequest) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var err error
	nullMetrics := &NullMetrics{}
	for _, req := range reqs {
		err = s.inMemory.Apply(zone.Id(), req, nullMetrics)
		if err != nil {
			break
		}
	}

	if err != nil {
		s.deleteZoneState(zone)
	}
}

func (s *zoneState) DeleteZoneState(zone DNSHostedZone) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.deleteZoneState(zone)
}

func (s *zoneState) deleteZoneState(zone DNSHostedZone) {
	delete(s.next, zone.Id())
	s.inMemory.DeleteZone(zone)
}

func (s *zoneState) RestrictCacheToZones(zones DNSHostedZones) {
	s.lock.Lock()
	defer s.lock.Unlock()

	obsoleteZoneIds := map[string]DNSHostedZone{}
	for _, zone := range s.inMemory.GetZones() {
		obsoleteZoneIds[zone.Id()] = zone
	}

	for _, zone := range zones {
		delete(obsoleteZoneIds, zone.Id())
	}

	for _, zone := range obsoleteZoneIds {
		s.deleteZoneState(zone)
	}
}
