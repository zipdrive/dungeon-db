import { ICellEditorComp, ICellEditorParams, ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import Choices, { EventChoice, InputChoice } from 'choices.js';
import { DropdownValue, queryAsync, SelectedHierarchicalListItemMetadata } from '../../api/query';
import classNames from 'classnames';
import { Channel } from '@tauri-apps/api/core';
import { MultiSelectDropdownCellContent, SingleSelectDropdownCellContent } from '../../api/model/cell';

type AdhocMultiSelectDropdownCellParams = ICellEditorParams & {
    multiSelectDropdown: MultiSelectDropdownCellContent,
    dropdownValues: DropdownValue[],
    onError: (e: unknown) => void,
};

export class AdhocMultiSelectCellEditor implements ICellEditorComp {
    private gui: HTMLDivElement;
    private choices: Choices;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('size-full');
        const select = document.createElement('select');
        select.multiple = true;
        this.gui.appendChild(select);

        this.choices = new Choices(select, {
            choices: [],
            classNames: {
                containerOuter: ['choices', 'size-full'],
                containerInner: ['choices__inner', 'border-none!', 'bg-transparent!'],
                input: ['choices__input'],
                inputCloned: ['choices__input--cloned'],
                list: ['choices__list'],
                listItems: ['choices__list--multiple'],
                listSingle: ['choices__list--single'],
                listDropdown: ['choices__list--dropdown'],
                item: ['choices__item'],
                itemSelectable: ['choices__item--selectable'],
                itemDisabled: ['choices__item--disabled'],
                itemChoice: ['choices__item--choice'],
                description: ['choices__description'],
                placeholder: ['choices__placeholder'],
                group: ['choices__group'],
                groupHeading: ['choices__heading'],
                button: ['choices__button'],
                activeState: ['is-active'],
                focusState: ['is-focused'],
                openState: ['is-open'],
                disabledState: ['is-disabled'],
                highlightedState: ['is-highlighted'],
                selectedState: ['is-selected'],
                flippedState: ['is-flipped'],
                loadingState: ['is-loading'],
                invalidState: ['is-invalid'],
                notice: ['choices__notice'],
                addChoice: ['choices__item--selectable', 'add-choice'],
                noResults: ['has-no-results'],
                noChoices: ['has-no-choices'],
            },
            
        });
    }

    init(params: AdhocMultiSelectDropdownCellParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    getValue() {
        const value = this.choices?.getValue();
        if (Array.isArray(value)) {
            console.log(value, 
                value.map((item) => typeof item.value === 'string' ? parseInt(item.value) : null), 
                value.map((item) => typeof item.value === 'string' ? parseInt(item.value) : null)
                    .filter((item) => item !== null && Number.isFinite(item))
            );
            if (value.length > 0) {
                return value.map((item) => typeof item.value === 'string' ? parseInt(item.value) : null)
                    .filter((item) => item !== null && Number.isFinite(item));
            }
        } else if (value) {
            const intValue = parseInt(value.value);
            return Number.isFinite(intValue) ? [intValue] : [];
        }
        return [];
    }

    refresh(params: AdhocMultiSelectDropdownCellParams): boolean {
        const multiSelectDropdown: MultiSelectDropdownCellContent = params.multiSelectDropdown;
        const dropdownValues = params.dropdownValues;

        this.choices.setChoices(
            dropdownValues.map((item) => {
                return {
                    value: item.value.toString(),
                    label: item.label,
                    selected: multiSelectDropdown.dropdownRowOid.indexOf(item.value) >= 0
                };
            }), 
            'value', 
            'label', 
            true
        );
        this.choices.showDropdown(false);
        return true;
    }
}