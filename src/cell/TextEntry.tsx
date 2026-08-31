import { CellDependency, CellIdentifier, TextEntryCellContent } from "../api/model/cell";
import { useCallback, useEffect, useRef, useState } from "react";
import { executeAsync } from "../api/action";
import classNames from "classnames";
import { EditCell, EditorBase, HyperFunc, isEnterKeyValue, isTab, timeout, VNode } from '@revolist/react-datagrid';


export class TextEntryEditor implements EditorBase {
    constructor(
        private content: TextEntryCellContent,
        private onError: (e: unknown) => void
    ) {}
    
    editInput: HTMLInputElement | null = null;

    element: Element | null = null;
    editCell?: EditCell = undefined;

    /**
     * Callback triggered on cell editor render
     */
    async componentDidRender(): Promise<void> {
        if (this.editInput) {
            await timeout();
            this.editInput?.focus();
        }
    }

    onKeyDown(e: KeyboardEvent) {
        const isEnter = isEnterKeyValue(e.key);
        const isKeyTab = isTab(e.key);

        if (
            (isKeyTab || isEnter) &&
            e.target &&
            !e.isComposing
        ) {
            // blur is needed to avoid autoscroll
            this.beforeDisconnect();
            // request callback which will close cell after all
            executeAsync({
                editCellContents: {
                    tableOid: this.content.dataTableOid,
                    columnOid: this.content.dataColumnOid,
                    rowOid: this.content.dataRowOid,
                    value: {
                        text: this.getValue() || null
                    }
                }
            })
            .catch((e) => {
                this.onError(e);
            });
        }
    }

    /**
     * IMPORTANT: Prevent scroll glitches when editor is closed and focus is on current input element.
     */
    beforeDisconnect() {
        this.editInput?.blur();
    }

    /**
     * Get value from input
     */
    getValue() {
        return this.editInput?.value;
    }

    /**
     * Render method for Editor plugin.
     * Renders input element with passed data from cell.
     * @param {Function} createElement - h function from stencil render.
     * @param {Object} _additionalData - additional data from plugin.
     * @returns {VNode} - input element.
     */
    render(createElement: HyperFunc<VNode>, _additionalData: any): VNode | VNode[] {
        return createElement(
            'input', 
            {
                type: 'text',
                enterKeyHint: 'enter',
                // set input value from cell data
                value: this.editCell?.val ?? '',
                // save input element as ref for further usage
                ref: (el: HTMLInputElement | null) => {
                    this.editInput = el;
                },
                // listen to keydown event on input element
                onKeyDown: (e: KeyboardEvent) => this.onKeyDown(e),
            }
        );
    }
}