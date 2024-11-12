/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import {
    ComponentEntity,
    CopyRequestContext,
    CopyResponseEntity,
    Dimensions,
    PasteRequest,
    PasteRequestContext,
    PasteRequestEntity
} from '../state/flow';
import { Observable } from 'rxjs';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { Position } from '../state/shared';
import { CanvasView } from './canvas-view.service';

@Injectable({
    providedIn: 'root'
})
export class CopyPasteService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private clusterConnectionService: ClusterConnectionService,
        private canvasView: CanvasView
    ) {}

    copy(copyRequest: CopyRequestContext): Observable<CopyResponseEntity> {
        return this.httpClient.post(
            `${CopyPasteService.API}/process-groups/${copyRequest.processGroupId}/copy`,
            copyRequest.copyRequestEntity
        );
    }

    paste(pasteRequest: PasteRequestContext): Observable<any> {
        const payload: PasteRequestEntity = {
            ...pasteRequest.pasteRequest,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
        };
        return this.httpClient.put(
            `${CopyPasteService.API}/process-groups/${pasteRequest.processGroupId}/paste`,
            payload
        );
    }

    public isCopiedContentInView(copyResponse: CopyResponseEntity): boolean {
        const bbox = this.calculateBoundingBoxForCopiedContent(copyResponse);
        return this.canvasView.isBoundingBoxInViewport(bbox, false);
    }

    /**
     * Use when pasting components to the same process group they were copied from and some
     * part of those components are still visible on canvas
     * @param copyResponse
     * @param pasteIncrement how many times the content has been pasted already. used to determine the overall offset.
     * @private
     */
    public toOffsetPasteRequest(copyResponse: CopyResponseEntity, pasteIncrement: number = 0): PasteRequest {
        const offset = 25;
        const paste: PasteRequest = {
            copyResponse: this.cloneCopyResponseEntity(copyResponse)
        };

        Object.values(paste.copyResponse).forEach((values: ComponentEntity[]) => {
            values.forEach((value) => {
                const newPos = this.canvasView.getCanvasPosition(value.position);
                value.position.x = (newPos?.x || value.position.x) + offset * (pasteIncrement + 1);
                value.position.y = (newPos?.y || value.position.y) + offset * (pasteIncrement + 1);
            });
        });

        return paste;
    }

    /**
     * Use when it isn't known if the copied content is still visible on the screen (possibly a different pg or browser tab),
     * or it is known to be off-screen.
     * @param copyResponse
     * @private
     */
    public toCenteredPasteRequest(copyResponse: CopyResponseEntity): PasteRequest {
        const paste: PasteRequest = {
            copyResponse: this.cloneCopyResponseEntity(copyResponse)
        };

        // get center of canvas
        const canvasBBox = this.canvasView.getCanvasBoundingClientRect();
        if (canvasBBox) {
            // Get the normalized center of the canvas to later compare with the center of the items being pasted
            const canvasCenterNormalized = this.canvasView.getCanvasPosition({
                x: canvasBBox.width / 2 + canvasBBox.left,
                y: canvasBBox.height / 2 + canvasBBox.top
            });
            if (canvasCenterNormalized) {
                // get the bounding box of the items being pasted (including the bends of connections)
                const copiedBBox = this.calculateBoundingBoxForCopiedContent(paste.copyResponse);

                // get it's center
                const centerOfCopiedContent: Position = {
                    x: copiedBBox.width / 2 + copiedBBox.x,
                    y: copiedBBox.height / 2 + copiedBBox.y
                };

                // find the difference between the centers
                const centerOffset: Position = {
                    x: canvasCenterNormalized.x - centerOfCopiedContent.x,
                    y: canvasCenterNormalized.y - centerOfCopiedContent.y
                };

                // offset all items (and bends) by the diff of the centers
                Object.values(paste.copyResponse).forEach((componentArray: any[]) => {
                    componentArray.forEach((component) => {
                        if (component.position) {
                            component.position.x += centerOffset.x;
                            component.position.y += centerOffset.y;
                        } else if (component.bends) {
                            component.bends.forEach((bend: Position) => {
                                bend.x += centerOffset.x;
                                bend.y += centerOffset.y;
                            });
                        }
                    });
                });

                // set the new bounding box on the request with a scale that would fit the contents
                paste.bbox = {
                    height: copiedBBox.height,
                    width: copiedBBox.width,
                    x: copiedBBox.x + centerOffset.x,
                    y: copiedBBox.y + centerOffset.y,
                    scale: Math.floor(
                        Math.min(canvasBBox.width / copiedBBox.width, canvasBBox.height / copiedBBox.height)
                    )
                };

                const willItFit = this.canvasView.isBoundingBoxInViewport(paste.bbox, true);
                if (!willItFit) {
                    paste.fitToScreen = true;
                }
            }
        }
        return paste;
    }

    private cloneCopyResponseEntity(copyResponse: CopyResponseEntity): CopyResponseEntity {
        const arrayOrUndefined = (arr: any[] | undefined) => {
            if (arr && Array.isArray(arr)) {
                arr = arr.map((component: any) => {
                    if (component.position) {
                        return {
                            ...component,
                            position: {
                                ...component.position
                            }
                        };
                    } else if (component.bends) {
                        return {
                            ...component,
                            bends: [
                                component.bends.map((bend: Position) => {
                                    return {
                                        ...bend
                                    };
                                })
                            ]
                        };
                    }
                });
                return arr;
            }
            return undefined;
        };
        return {
            connections: arrayOrUndefined(copyResponse.connections),
            funnels: arrayOrUndefined(copyResponse.funnels),
            inputPorts: arrayOrUndefined(copyResponse.inputPorts),
            labels: arrayOrUndefined(copyResponse.labels),
            outputPorts: arrayOrUndefined(copyResponse.outputPorts),
            processGroups: arrayOrUndefined(copyResponse.processGroups),
            processors: arrayOrUndefined(copyResponse.processors),
            remoteProcessGroups: arrayOrUndefined(copyResponse.remoteProcessGroups)
        } as CopyResponseEntity;
    }

    private calculateBoundingBoxForCopiedContent(copyResponse: CopyResponseEntity): any {
        const bbox = {
            left: Number.MAX_SAFE_INTEGER,
            top: Number.MAX_SAFE_INTEGER,
            right: 0,
            bottom: 0
        };
        Object.values(copyResponse)
            .flat()
            .reduce((acc, current) => {
                const dimensions: Dimensions = this.getComponentWidth(current);
                if (current.componentType === 'CONNECTION') {
                    current.bends.forEach((bend: Position) => {
                        acc.left = Math.min(acc.left, bend.x);
                        acc.top = Math.min(acc.top, bend.y);
                        acc.right = Math.max(acc.right, bend.x);
                        acc.right = Math.max(acc.bottom, bend.y);
                    });
                } else {
                    acc.left = Math.min(acc.left, current.position.x);
                    acc.top = Math.min(acc.top, current.position.y);
                    acc.right = Math.max(acc.right, current.position.x + dimensions.width);
                    acc.bottom = Math.max(acc.bottom, current.position.y + dimensions.height);
                }
                return acc;
            }, bbox);

        return {
            x: bbox.left,
            y: bbox.top,
            width: bbox.right - bbox.left,
            height: bbox.bottom - bbox.top
        };
    }

    private getComponentWidth(component: any): Dimensions {
        switch (component.componentType) {
            case 'PROCESSOR':
                return {
                    width: 352,
                    height: 128
                };
            case 'PROCESS_GROUP':
            case 'REMOTE_PROCESS_GROUP':
                return {
                    width: 384,
                    height: 176
                };
            case 'INPUT_PORT':
            case 'OUTPUT_PORT':
            case 'REMOTE_INPUT_PORT':
            case 'REMOTE_OUTPUT_PORT':
                return {
                    width: 240,
                    height: 48
                };
            case 'FUNNEL':
                return { height: 48, width: 48 };
            case 'LABEL':
                return { height: component.height, width: component.width };
            default:
                return { height: 0, width: 0 };
        }
    }
}
