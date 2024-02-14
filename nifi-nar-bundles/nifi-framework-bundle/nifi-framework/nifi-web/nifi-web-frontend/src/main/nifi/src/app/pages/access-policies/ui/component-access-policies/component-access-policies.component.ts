/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Store } from '@ngrx/store';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import {
    createAccessPolicy,
    openAddTenantToPolicyDialog,
    promptOverrideAccessPolicy,
    promptDeleteAccessPolicy,
    promptRemoveTenantFromPolicy,
    reloadAccessPolicy,
    resetAccessPolicyState,
    selectComponentAccessPolicy,
    setAccessPolicy
} from '../../state/access-policy/access-policy.actions';
import { AccessPolicyState, RemoveTenantFromPolicyRequest } from '../../state/access-policy';
import { initialState } from '../../state/access-policy/access-policy.reducer';
import {
    selectAccessPolicyState,
    selectComponentResourceActionFromRoute
} from '../../state/access-policy/access-policy.selectors';
import { distinctUntilChanged } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { NiFiCommon } from '../../../../service/nifi-common.service';
import { ComponentType, isDefinedAndNotNull, SelectOption, TextTipInput } from '../../../../state/shared';
import { TextTip } from '../../../../ui/common/tooltips/text-tip/text-tip.component';
import { AccessPolicyEntity, Action, PolicyStatus, ResourceAction } from '../../state/shared';
import { loadFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.actions';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { loadTenants, resetTenantsState } from '../../state/tenants/tenants.actions';
import { loadPolicyComponent, resetPolicyComponentState } from '../../state/policy-component/policy-component.actions';
import { selectPolicyComponentState } from '../../state/policy-component/policy-component.selectors';
import { PolicyComponentState } from '../../state/policy-component';

@Component({
    selector: 'global-access-policies',
    templateUrl: './component-access-policies.component.html',
    styleUrls: ['./component-access-policies.component.scss']
})
export class ComponentAccessPolicies implements OnInit, OnDestroy {
    flowConfiguration$ = this.store.select(selectFlowConfiguration);
    accessPolicyState$ = this.store.select(selectAccessPolicyState);
    policyComponentState$ = this.store.select(selectPolicyComponentState);
    currentUser$ = this.store.select(selectCurrentUser);

    protected readonly TextTip = TextTip;
    protected readonly Action = Action;
    protected readonly PolicyStatus = PolicyStatus;
    protected readonly ComponentType = ComponentType;

    policyForm: FormGroup;
    policyActionOptions: SelectOption[] = [
        {
            text: 'view the component',
            value: 'read-component',
            description: 'Allows users to view component configuration details'
        },
        {
            text: 'modify the component',
            value: 'write-component',
            description: 'Allows users to modify component configuration details'
        },
        {
            text: 'operate the component',
            value: 'write-operation',
            description:
                'Allows users to operate components by changing component run status (start/stop/enable/disable), remote port transmission status, or terminating processor threads'
        },
        {
            text: 'view provenance',
            value: 'read-provenance-data',
            description: 'Allows users to view provenance events generated by this component'
        },
        {
            text: 'view the data',
            value: 'read-data',
            description:
                'Allows users to view metadata and content for this component in flowfile queues in outbound connections and through provenance events'
        },
        {
            text: 'modify the data',
            value: 'write-data',
            description:
                'Allows users to empty flowfile queues in outbound connections and submit replays through provenance events'
        },
        {
            text: 'receive data via site-to-site',
            value: 'write-receive-data',
            description: 'Allows this port to receive data from these NiFi instances',
            disabled: true
        },
        {
            text: 'send data via site-to-site',
            value: 'write-send-data',
            description: 'Allows this port to send data to these NiFi instances',
            disabled: true
        },
        {
            text: 'view the policies',
            value: 'read-policies',
            description: 'Allows users to view the list of users who can view/modify this component'
        },
        {
            text: 'modify the policies',
            value: 'write-policies',
            description: 'Allows users to modify the list of users who can view/modify this component'
        }
    ];

    action!: Action;
    resource!: string;
    policy!: string;
    resourceIdentifier!: string;

    @ViewChild('inheritedFromPolicies') inheritedFromPolicies!: TemplateRef<any>;
    @ViewChild('inheritedFromController') inheritedFromController!: TemplateRef<any>;
    @ViewChild('inheritedFromGlobalParameterContexts') inheritedFromGlobalParameterContexts!: TemplateRef<any>;
    @ViewChild('inheritedFromProcessGroup') inheritedFromProcessGroup!: TemplateRef<any>;

    constructor(
        private store: Store<AccessPolicyState>,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.policyForm = this.formBuilder.group({
            policyAction: new FormControl(this.policyActionOptions[0].value, Validators.required)
        });

        this.store
            .select(selectComponentResourceActionFromRoute)
            .pipe(
                isDefinedAndNotNull(),
                distinctUntilChanged((a, b) => {
                    return (
                        a.action == b.action &&
                        a.policy == b.policy &&
                        a.resource == b.resource &&
                        a.resourceIdentifier == b.resourceIdentifier
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe((componentResourceAction) => {
                if (componentResourceAction) {
                    this.action = componentResourceAction.action;
                    this.policy = componentResourceAction.policy;
                    this.resource = componentResourceAction.resource;
                    this.resourceIdentifier = componentResourceAction.resourceIdentifier;

                    // data transfer policies for site to site are presented different in the form so
                    // we need to distinguish by type
                    let policyForResource: string = this.policy;
                    if (this.policy === 'data-transfer') {
                        if (this.resource === 'input-ports') {
                            policyForResource = 'receive-data';
                        } else {
                            policyForResource = 'send-data';
                        }
                    }

                    this.policyForm.get('policyAction')?.setValue(`${this.action}-${policyForResource}`);

                    // component policies are presented simply as '/processors/1234' while non-component policies
                    // like viewing provenance for a specific component is presented as `/provenance-data/processors/1234`
                    let resourceToLoad: string = this.resource;
                    if (componentResourceAction.policy !== 'component') {
                        resourceToLoad = `${this.policy}/${this.resource}`;
                    }

                    const resourceAction: ResourceAction = {
                        action: this.action,
                        resource: resourceToLoad,
                        resourceIdentifier: this.resourceIdentifier
                    };

                    this.store.dispatch(
                        loadPolicyComponent({
                            request: {
                                componentResourceAction
                            }
                        })
                    );
                    this.store.dispatch(
                        setAccessPolicy({
                            request: {
                                resourceAction
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadFlowConfiguration());
        this.store.dispatch(loadTenants());
    }

    isInitialLoading(state: AccessPolicyState): boolean {
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    isComponentPolicy(option: SelectOption, policyComponentState: PolicyComponentState): boolean {
        // consider the type of component to override which policies shouldn't be supported

        if (policyComponentState.resource === 'process-groups') {
            switch (option.value) {
                case 'write-send-data':
                case 'write-receive-data':
                    return false;
            }
        } else if (
            policyComponentState.resource === 'controller-services' ||
            policyComponentState.resource === 'reporting-tasks'
        ) {
            switch (option.value) {
                case 'read-data':
                case 'write-data':
                case 'write-send-data':
                case 'write-receive-data':
                case 'read-provenance-data':
                    return false;
            }
        } else if (
            policyComponentState.resource === 'parameter-contexts' ||
            policyComponentState.resource === 'parameter-providers'
        ) {
            switch (option.value) {
                case 'read-data':
                case 'write-data':
                case 'write-send-data':
                case 'write-receive-data':
                case 'read-provenance-data':
                case 'write-operation':
                    return false;
            }
        } else if (policyComponentState.resource === 'labels') {
            switch (option.value) {
                case 'write-operation':
                case 'read-data':
                case 'write-data':
                case 'write-send-data':
                case 'write-receive-data':
                    return false;
            }
        } else if (policyComponentState.resource === 'input-ports' && policyComponentState.allowRemoteAccess) {
            // if input ports allow remote access, disable send data. if input ports do not allow remote
            // access it will fall through to the else block where both send and receive data will be disabled
            switch (option.value) {
                case 'write-send-data':
                    return false;
            }
        } else if (policyComponentState.resource === 'output-ports' && policyComponentState.allowRemoteAccess) {
            // if output ports allow remote access, disable receive data. if output ports do not allow remote
            // access it will fall through to the else block where both send and receive data will be disabled
            switch (option.value) {
                case 'write-receive-data':
                    return false;
            }
        } else {
            switch (option.value) {
                case 'write-send-data':
                case 'write-receive-data':
                    return false;
            }
        }

        // enable all other options
        return true;
    }

    getSelectOptionTipData(option: SelectOption): TextTipInput {
        return {
            // @ts-ignore
            text: option.description
        };
    }

    getContextIcon(): string {
        switch (this.resource) {
            case 'processors':
                return 'icon-processor';
            case 'input-ports':
                return 'icon-port-in';
            case 'output-ports':
                return 'icon-port-out';
            case 'funnels':
                return 'icon-funnel';
            case 'labels':
                return 'icon-label';
            case 'remote-process-groups':
                return 'icon-group-remote';
            case 'parameter-providers':
            case 'parameter-contexts':
                return 'icon-drop';
        }

        return 'icon-group';
    }

    getContextType(): string {
        switch (this.resource) {
            case 'processors':
                return 'Processor';
            case 'input-ports':
                return 'Input Ports';
            case 'output-ports':
                return 'Output Ports';
            case 'funnels':
                return 'Funnel';
            case 'labels':
                return 'Label';
            case 'remote-process-groups':
                return 'Remote Process Group';
            case 'parameter-contexts':
                return 'Parameter Contexts';
            case 'parameter-providers':
                return 'Parameter Provider';
        }

        return 'Process Group';
    }

    policyActionChanged(value: string): void {
        let action: Action;
        let policy: string;

        switch (value) {
            case 'read-component':
                action = Action.Read;
                policy = 'component';
                break;
            case 'write-component':
                action = Action.Write;
                policy = 'component';
                break;
            case 'write-operation':
                action = Action.Write;
                policy = 'operation';
                break;
            case 'read-data':
                action = Action.Read;
                policy = 'data';
                break;
            case 'write-data':
                action = Action.Write;
                policy = 'data';
                break;
            case 'read-provenance-data':
                action = Action.Read;
                policy = 'provenance-data';
                break;
            case 'read-policies':
                action = Action.Read;
                policy = 'policies';
                break;
            case 'write-policies':
                action = Action.Write;
                policy = 'policies';
                break;
            default:
                action = Action.Write;
                policy = 'data-transfer';
                break;
        }

        this.store.dispatch(
            selectComponentAccessPolicy({
                request: {
                    resourceAction: {
                        action,
                        policy,
                        resource: this.resource,
                        resourceIdentifier: this.resourceIdentifier
                    }
                }
            })
        );
    }

    getTemplateForInheritedPolicy(policy: AccessPolicyEntity): TemplateRef<any> {
        if (policy.component.resource.startsWith('/policies')) {
            return this.inheritedFromPolicies;
        } else if (policy.component.resource === '/controller') {
            return this.inheritedFromController;
        } else if (policy.component.resource === '/parameter-contexts') {
            return this.inheritedFromGlobalParameterContexts;
        }

        return this.inheritedFromProcessGroup;
    }

    getInheritedProcessGroupRoute(policy: AccessPolicyEntity): string[] {
        return ['/process-groups', this.nifiCommon.substringAfterLast(policy.component.resource, '/')];
    }

    createNewPolicy(): void {
        this.store.dispatch(createAccessPolicy());
    }

    overridePolicy(): void {
        this.store.dispatch(promptOverrideAccessPolicy());
    }

    removeTenantFromPolicy(request: RemoveTenantFromPolicyRequest): void {
        this.store.dispatch(
            promptRemoveTenantFromPolicy({
                request
            })
        );
    }

    addTenantToPolicy(): void {
        this.store.dispatch(openAddTenantToPolicyDialog());
    }

    deletePolicy(): void {
        this.store.dispatch(promptDeleteAccessPolicy());
    }

    refreshGlobalAccessPolicy(): void {
        this.store.dispatch(reloadAccessPolicy());
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetAccessPolicyState());
        this.store.dispatch(resetTenantsState());
        this.store.dispatch(resetPolicyComponentState());
    }
}
