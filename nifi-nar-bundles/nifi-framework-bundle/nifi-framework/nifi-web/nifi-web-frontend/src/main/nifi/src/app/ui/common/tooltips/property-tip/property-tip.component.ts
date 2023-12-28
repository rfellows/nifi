/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, Input } from '@angular/core';
import { NgForOf, NgIf } from '@angular/common';
import { PropertyDescriptor, PropertyTipInput } from '../../../../state/shared';
import { NiFiCommon } from '../../../../service/nifi-common.service';
import { ControllerServiceApi } from '../../controller-service/controller-service-api/controller-service-api.component';

@Component({
    selector: 'property-tip',
    standalone: true,
    templateUrl: './property-tip.component.html',
    imports: [NgForOf, NgIf, ControllerServiceApi],
    styleUrls: ['./property-tip.component.scss']
})
export class PropertyTip {
    @Input() left = 0;
    @Input() top = 0;
    @Input() data: PropertyTipInput | undefined;

    constructor(private nifiCommon: NiFiCommon) {}

    hasDescription(descriptor: PropertyDescriptor): boolean {
        return !this.nifiCommon.isBlank(descriptor.description);
    }

    hasDefaultValue(descriptor: PropertyDescriptor): boolean {
        return !this.nifiCommon.isBlank(descriptor.defaultValue);
    }

    identifiesControllerService(descriptor: PropertyDescriptor): boolean {
        return !this.nifiCommon.isBlank(descriptor.identifiesControllerService);
    }
}
