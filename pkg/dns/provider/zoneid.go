/*
 * Copyright 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *
 */

package provider

import "strings"

type QualifiedZoneID struct {
	providerType string
	zoneID       string
}

func NewQualifiedZoneID(providerType, zoneID string) QualifiedZoneID {
	return QualifiedZoneID{providerType: providerType, zoneID: zoneID}
}

func NewQualifiedZoneIDPtr(providerType, zoneID *string) *QualifiedZoneID {
	if providerType == nil || zoneID == nil {
		return nil
	}
	if len(*providerType) == 0 || len(*zoneID) == 0 {
		return nil
	}
	return &QualifiedZoneID{providerType: *providerType, zoneID: *zoneID}
}

func ParseQualifiedZoneID(s string) *QualifiedZoneID {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	zoneID := NewQualifiedZoneID(parts[0], parts[1])
	return &zoneID
}

func (q QualifiedZoneID) ProviderType() string {
	return q.providerType
}

func (q QualifiedZoneID) ZoneID() string {
	return q.zoneID
}

func (q QualifiedZoneID) String() string {
	return q.providerType + ":" + q.zoneID
}
