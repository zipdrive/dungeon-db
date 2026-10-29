import React, { useEffect } from "react";
import { TableCellReference } from "../../../api/model/reportRow";
import { TableRowLabel } from "../../../api/model/tableRow";
import AsyncSelect from 'react-select/async';
import { SingleValue } from 'react-select';
import { executeAsync } from "../../../api/action";

export type SingleSelectDropdownCellEditorProps = {
    tableOid: number,
    value: number | null,
    cellRef: TableCellReference,
    items: Promise<TableRowLabel[]>,
    closeEditor: () => void,
    onError: (e: unknown) => void
};

export function SingleSelectDropdownCellEditor(props: SingleSelectDropdownCellEditorProps): React.JSX.Element {
    const [value, setValue] = React.useState<TableRowLabel | null>(null);

    // Sets the value of the dropdown.
    useEffect(() => {
        props.items.then((items) => {
            setValue(items.find(({ oid }) => oid == props.value) ?? null);
        });
    }, [props.value, props.items]);

    /**
     * Promise to load options.
     */
    const loadOptions = React.useCallback(async (inputValue: string) => {
        const items = await props.items;
        return items.filter(({ label }) => label.includes(inputValue));
    }, [props.value, props.items]);

    /**
     * Sets the new value when the dropdown's selection is changed.
     */
    const onChange = React.useCallback((newLabel: SingleValue<TableRowLabel>) => {
        setValue(newLabel);
    }, []);

    /**
     * Commit changes when the dropdown is unfocused.
     */
    const onBlur = React.useCallback(async () => {
        try {
            await executeAsync({
                editTableCellContents: {
                    ...props.cellRef,
                    content: {
                        singleSelectDropdown: {
                            tableOid: props.tableOid,
                            value: value?.oid ?? null
                        }
                    }
                }
            });
        } catch (e) {
            props.onError(e);
        }
        props.closeEditor();
    }, [props.tableOid, value, props.cellRef, props.onError, props.closeEditor]);

    return <AsyncSelect
        className="dngrid-editor-select absolute top-0 bottom-0 left-0 right-0"
        autoFocus
        theme={(theme) => ({
            ...theme,
            borderRadius: 0,
            colors: {
                primary: 'rgb(var(--color-primary)/1)',
                primary75: 'color-mix(in oklch, rgb(var(--color-primary)/1) 75%, rgb(var(--color-surface-light)/1) 25%)',
                primary50: 'color-mix(in oklch, rgb(var(--color-primary)/1) 50%, rgb(var(--color-surface-light)/1) 50%)',
                primary25: 'color-mix(in oklch, rgb(var(--color-primary)/1) 25%, rgb(var(--color-surface-light)/1) 75%)',
                danger: 'rgb(var(--color-error)/1)',
                dangerLight: 'rgb(var(--color-error-light)/1)',
                neutral0: 'rgb(var(--color-surface-light)/1)',
                neutral5: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 5%, rgb(var(--color-surface-light)/1) 95%)',
                neutral10: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 10%, rgb(var(--color-surface-light)/1) 90%)',
                neutral20: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 20%, rgb(var(--color-surface-light)/1) 80%)',
                neutral30: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 30%, rgb(var(--color-surface-light)/1) 70%)',
                neutral40: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 40%, rgb(var(--color-surface-light)/1) 60%)',
                neutral50: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 50%, rgb(var(--color-surface-light)/1) 50%)',
                neutral60: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 60%, rgb(var(--color-surface-light)/1) 40%)',
                neutral70: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 70%, rgb(var(--color-surface-light)/1) 30%)',
                neutral80: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 80%, rgb(var(--color-surface-light)/1) 20%)',
                neutral90: 'color-mix(in oklch, rgb(var(--color-foreground)/1) 90%, rgb(var(--color-surface-light)/1) 10%)'
            }
        })}
        styles={{
            container: (baseStyles, state) => ({
                ...baseStyles,
                position: 'absolute'
            }),
            control: (baseStyles, state) => ({
                ...baseStyles,
                borderRadius: 0,
                border: 'none',
                outline: 'none',
                boxShadow: 'none',
                width: '100%',
                height: '100%'
            }),
        }}
        value={value}
        onChange={onChange}
        onBlur={onBlur}
        isClearable
        isSearchable
        defaultMenuIsOpen
        defaultOptions
        loadOptions={loadOptions}
    />;
}