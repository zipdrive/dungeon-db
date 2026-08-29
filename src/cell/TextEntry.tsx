import { CellDependency, CellIdentifier, TextEntryCellContent } from "../api/model/cell";
import { useCallback, useEffect, useRef, useState } from "react";
import { executeAsync } from "../api/action";
import classNames from "classnames";
import { EditorBase, HyperFunc, VNode } from '@revolist/react-datagrid';


export class TextEntryEditor implements EditorBase {
    #content: TextEntryCellContent;
    

    constructor(content: TextEntryCellContent) {
        this.#content = content;
    }
    
    editInput: HTMLInputElement | null = null;

    element: Element | null = null;
    editCell?: EditCell = undefined;

    constructor(
        public data: ColumnDataSchemaModel,
        private saveCallback?: SaveCallback,
    ) {}

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
            this.saveCallback &&
            !e.isComposing
        ) {
            // blur is needed to avoid autoscroll
            this.beforeDisconnect();
            // request callback which will close cell after all
            this.saveCallback(this.getValue(), isKeyTab);
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



export type TextEntryCellProps = {
    content: TextEntryCellContent,
    isFocused: boolean,
    onSetFocused: () => void,
    isSelected: boolean,
    listener: (CellIdentifier: CellIdentifier, isolatedCellDependencies: CellDependency[]) => (() => void),
    onError: (e: unknown) => void,
};

export function TextEntryCell(props: TextEntryCellProps): React.JSX.Element {
    const [isEditMode, setEditMode] = useState<boolean>(false);
    const [inputValue, setInputValue] = useState<string>('');
    const escapePressedRef = useRef<boolean>(false);

    useEffect(() => {
        setInputValue(props.content.label || '');
    }, [props.content.label]);

    useEffect(() => {
        return props.listener(props.content.cellIdentifier, props.content.isolatedCellDependencies);
    }, [props.listener, props.content.cellIdentifier, props.content.isolatedCellDependencies]);

    const onCommit = useCallback(async (newLabel: string) => {
        try {
            await executeAsync({
                editCellContents: {
                    tableOid: props.content.dataTableOid,
                    columnOid: props.content.dataColumnOid,
                    rowOid: props.content.dataRowOid,
                    value: {
                        text: newLabel ? newLabel : null
                    }
                }
            });
        } catch (e) {
            props.onError(e);
        }
    }, [props.content.dataTableOid, props.content.dataColumnOid, props.content.dataRowOid, props.onError]);

    return (<div
        className={classNames(
            `column${props.content.cellIdentifier.columnOid}`
        )}
        onKeyDown={(e) => {
            if (!isEditMode && isValidKey(e, [])) {
                e.stopPropagation();
                setInputValue('');
                setEditMode(true);
            } else if (!isEditMode && !props.isSelected && (e.key === "Enter" || e.key === "F2")) {
                e.stopPropagation();
                setInputValue(props.content.label || '');
                setEditMode(true);
            }
        }}
        tabIndex={0}
    >
        {isEditMode 
        ? (<input 
            value={inputValue}
            onChange={(e) => { setInputValue(e.target.value); }}
            onBlur={(e) => {
                if (!escapePressedRef.current) {
                    onCommit(e.currentTarget.value);
                }
                setEditMode(false);
                if (escapePressedRef.current) {
                    escapePressedRef.current = false;
                }
            }}
            onCut={(e) => e.stopPropagation()}
            onCopy={(e) => e.stopPropagation()}
            onPaste={(e) => e.stopPropagation()}
            onPointerDown={(e) => e.stopPropagation()}
            onKeyDown={(e) => {
                const controlKeys = ["Escape", "Enter", "Tab"];
                if (!controlKeys.includes(e.key)) {
                    e.stopPropagation();
                }
                if (e.key === "Escape") {
                    escapePressedRef.current = true;
                    setEditMode(false);
                } else if (e.key === "Enter") {
                    setEditMode(false);
                }
            }}
            autoFocus
        />)
        : props.content.label}
    </div>);
}