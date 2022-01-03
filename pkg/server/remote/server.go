/*
 * Copyright 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
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

package remote

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gardener/controller-manager-library/pkg/logger"
	"github.com/gardener/controller-manager-library/pkg/resources"
	"github.com/gardener/external-dns-management/pkg/dns/provider"
	"github.com/gardener/external-dns-management/pkg/server/metrics"
	"github.com/gardener/external-dns-management/pkg/server/remote/common"
	"github.com/gardener/external-dns-management/pkg/server/remote/conversion"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type server struct {
	lock            sync.Mutex
	serverID        string
	spinning        time.Duration
	logctx          logger.LogContext
	namespaceStates map[string]*namespaceState

	tokenTTL           time.Duration
	tokenCleanupTicker *time.Ticker

	common.UnimplementedRemoteProviderServer
}

func CreateServer(logctx logger.LogContext) common.RemoteProviderServer {
	return newServer(logctx)
}

func newServer(logctx logger.LogContext) *server {
	id, _ := randonString(8)
	s := &server{
		serverID:        id,
		spinning:        15 * time.Second,
		logctx:          logctx,
		namespaceStates: map[string]*namespaceState{},
		tokenTTL:        2 * time.Hour,
	}

	s.tokenCleanupTicker = time.NewTicker(s.tokenTTL)
	go func() {
		for range s.tokenCleanupTicker.C {
			s.cleanupTokens()
		}
	}()

	return s
}

func (s *server) ProviderUpdatedEvent(logger logger.LogContext, objectName resources.ObjectName, annotations map[string]string, handler provider.LightDNSHandler) {
	remoteAccess := false
	if annotations != nil {
		if remoteAccessAnnotation, ok := annotations[provider.AnnotationRemoteAccess]; ok {
			remoteAccess, _ = strconv.ParseBool(remoteAccessAnnotation)
		}
	}
	if !remoteAccess {
		s.ProviderRemovedEvent(logger, objectName)
		return
	}

	nsState := s.getNamespaceState(objectName.Namespace(), true)
	if nsState.updateHandler(objectName.Name(), handler) {
		logger.Infof("added/updated for remote access")
	}
}

func (s *server) getNamespaceState(ns string, createIfNeeded bool) *namespaceState {
	s.lock.Lock()
	defer s.lock.Unlock()

	nsState := s.namespaceStates[ns]
	if nsState == nil && createIfNeeded {
		nsState = newNamespaceState(ns)
		s.namespaceStates[ns] = nsState
		return nsState
	}

	if nsState == nil || len(nsState.handlers) == 0 {
		return nil
	}
	return nsState
}

func (s *server) ProviderRemovedEvent(logger logger.LogContext, objectName resources.ObjectName) {
	nsState := s.getNamespaceState(objectName.Namespace(), false)
	if nsState == nil {
		return
	}

	if nsState.removeHandler(objectName.Name()) {
		logger.Infof("removing provider from remote access")
	}
}

func (s *server) checkAuth(token, requestType, zoneid string) (*namespaceState, logger.LogContext, error) {
	parts := strings.SplitN(token, "|", 2)
	namespace := parts[0]
	nsState := s.getNamespaceState(namespace, false)
	if nsState == nil {
		return nil, s.logctx, fmt.Errorf("namespace %s not found or no providers available", namespace)
	}

	commonName, err := nsState.getToken(token)
	logctx := s.logctx.NewContext("namespace", nsState.name).NewContext("commonName", commonName)
	if err != nil {
		return nil, logctx, err
	}
	metrics.ReportRemoteAccessRequests(namespace, commonName, requestType, zoneid)
	return nsState, logctx, nil
}

func (s *server) Login(ctx context.Context, request *common.LoginRequest) (*common.LoginResponse, error) {
	commonName, err := s.checkNamespaceAuthorization(ctx, request.Namespace)
	logctx := s.logctx.NewContext("namespace", request.Namespace).NewContext("commonName", commonName)
	if err != nil {
		logctx.Warn("Login auth failed")
		return nil, err
	}
	logctx.Info("Login auth successful")

	nsState := s.getNamespaceState(request.Namespace, false)
	if nsState == nil {
		metrics.ReportRemoteAccessLogins(request.Namespace, commonName, false)
		logctx.Info("namespace %s not found or no providers available", request.Namespace)
		return nil, fmt.Errorf("namespace %s not found or no providers available", request.Namespace)
	}

	metrics.ReportRemoteAccessLogins(request.Namespace, commonName, true)

	rnd, err := randonString(16)
	if err != nil {
		return nil, fmt.Errorf("random failed: %w", err)
	}

	token := nsState.generateAndAddToken(s.tokenTTL, rnd, commonName, s.serverID)
	return &common.LoginResponse{Token: token}, nil
}

func (s *server) checkNamespaceAuthorization(ctx context.Context, namespace string) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "no peer found")
	}

	tlsAuth, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "unexpected peer transport credentials")
	}

	if len(tlsAuth.State.VerifiedChains) == 0 || len(tlsAuth.State.VerifiedChains[0]) == 0 {
		return "", status.Error(codes.Unauthenticated, "could not verify peer certificate")
	}

	commonName := tlsAuth.State.VerifiedChains[0][0].Subject.CommonName
	firstLabel := strings.SplitN(commonName, ".", 2)[0]
	// Check subject common name against configured username
	if firstLabel != "*" && firstLabel != namespace {
		return commonName, status.Error(codes.Unauthenticated, "invalid subject common name")
	}

	return commonName, nil
}

func (s *server) cleanupTokens() {
	s.lock.Lock()
	defer s.lock.Unlock()

	count := 0
	now := time.Now()
	for _, nsState := range s.namespaceStates {
		count += nsState.cleanupTokens(now)
	}
	s.logctx.Infof("token cleanup of %d outdated tokens", count)
}

func (s *server) GetZones(_ context.Context, request *common.GetZonesRequest) (*common.Zones, error) {
	nsState, logctx, err := s.checkAuth(request.Token, "GetZones", "")
	if err != nil {
		logctx.Warn(err)
		return nil, err
	}
	logctx.Info("GetZones")

	zones, err := nsState.getAllZones(s.spinning)
	if err != nil {
		return nil, err
	}

	result := &common.Zones{}
	for _, zone := range zones {
		z := &common.Zone{
			Id:              zone.Id(),
			ProviderType:    zone.ProviderType(),
			Key:             zone.Key(),
			Domain:          zone.Domain(),
			ForwardedDomain: zone.ForwardedDomains(),
			PrivateZone:     zone.IsPrivate(),
		}
		result.Zone = append(result.Zone, z)
	}

	logctx.Infof("GetZones: %d zones", len(result.Zone))
	return result, nil
}

func (s *server) GetZoneState(_ context.Context, request *common.GetZoneStateRequest) (*common.ZoneState, error) {
	nsState, logctx, err := s.checkAuth(request.Token, "GetZoneState", request.Zoneid)
	if err != nil {
		logctx.Warn(err)
		return nil, err
	}
	logctx = logctx.NewContext("zoneid", request.Zoneid)
	logctx.Info("GetZoneState")

	hstate, zone, err := nsState.lockupZone(s.spinning, request.Zoneid)
	if err != nil {
		return nil, err
	}
	if !hstate.lock.TryLockSpinning(s.spinning) {
		logctx.Info("rejected - busy")
		return nil, fmt.Errorf("busy")
	}
	defer hstate.lock.Unlock()

	state, err := hstate.handler.GetZoneState(zone)
	if err != nil {
		return nil, err
	}
	result := &common.ZoneState{DnsSets: conversion.MarshalDNSSets(state.GetDNSSets())}
	logctx.Infof("GetZoneState: %d DNSSets", len(result.GetDnsSets()))

	return result, nil
}

func (s *server) Execute(_ context.Context, request *common.ExecuteRequest) (*common.ExecuteResponse, error) {
	nsState, logctx, err := s.checkAuth(request.Token, "Execute", request.Zoneid)
	if err != nil {
		logctx.Warn(err)
		return nil, err
	}
	logctx = logctx.NewContext("zoneid", request.Zoneid)
	logctx.Infof("Execute: %d changes", len(request.ChangeRequest))

	hstate, zone, err := nsState.lockupZone(s.spinning, request.Zoneid)
	if err != nil {
		return nil, err
	}

	if !hstate.lock.TryLockSpinning(s.spinning) {
		logctx.Info("rejected - busy")
		return nil, fmt.Errorf("busy")
	}
	defer hstate.lock.Unlock()

	state, err := hstate.handler.GetZoneState(zone)
	if err != nil {
		return nil, err
	}

	memLogger := newMemoryLogger(logctx)
	var requests []*provider.ChangeRequest
	var responses []*common.ChangeResponse
	for _, request := range request.ChangeRequest {
		response := &common.ChangeResponse{}
		responses = append(responses, response)
		done := newDoneHandler(response)
		req, err := conversion.UnmarshalChangeRequest(request, done)
		if err != nil {
			return nil, err
		}
		requests = append(requests, req)
	}
	err = hstate.handler.ExecuteRequests(memLogger, zone, state, requests)
	return &common.ExecuteResponse{
		ChangeResponse: responses,
		LogMessage:     memLogger.entries,
	}, err
}

func randonString(len int) (string, error) {
	tokenRnd := make([]byte, len)
	_, err := rand.Read(tokenRnd)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(tokenRnd), nil
}

func newDoneHandler(response *common.ChangeResponse) provider.DoneHandler {
	return &serverDoneHandler{response: response}
}

type serverDoneHandler struct {
	response *common.ChangeResponse
}

func (dh *serverDoneHandler) Succeeded() {
	dh.response.State = common.ChangeResponse_SUCCEEDED
}

func (dh *serverDoneHandler) SetInvalid(err error) {
	dh.response.State = common.ChangeResponse_INVALID
	dh.response.ErrorMessage = err.Error()
}

func (dh *serverDoneHandler) Failed(err error) {
	dh.response.State = common.ChangeResponse_FAILED
	dh.response.ErrorMessage = err.Error()
}

func (dh *serverDoneHandler) Throttled() {
	dh.response.State = common.ChangeResponse_THROTTLED
}
