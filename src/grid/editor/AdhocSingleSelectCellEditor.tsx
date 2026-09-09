import { ICellEditorComp, ICellEditorParams, ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import Choices, { EventChoice, InputChoice } from 'choices.js';
import { DropdownValue, queryAsync, SelectedHierarchicalListItemMetadata } from '../../api/query';
import classNames from 'classnames';
import { Channel } from '@tauri-apps/api/core';
import { SingleSelectDropdownCellContent } from '../../api/model/cell';

type AdhocSingleSelectDropdownCellParams = ICellEditorParams & {
    singleSelectDropdown: SingleSelectDropdownCellContent,
    dropdownValues: DropdownValue[],
    onError: (e: unknown) => void,
};

export class AdhocSingleSelectCellEditor implements ICellEditorComp {
    private gui: HTMLDivElement;
    private choices: Choices;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('size-full');
        const select = document.createElement('select');
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
            }
        });
    }

    init(params: AdhocSingleSelectDropdownCellParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    getValue() {
        const value = this.choices?.getValue();
        if (Array.isArray(value)) {
            if (value.length > 0) {
                const intValue = parseInt(value[0].value);
                return Number.isFinite(intValue) ? intValue : null;
            }
        } else if (value) {
            const intValue = parseInt(value.value);
            return Number.isFinite(intValue) ? intValue : null;
        }
        return null;
    }

    refresh(params: AdhocSingleSelectDropdownCellParams): boolean {
        const singleSelectDropdown: SingleSelectDropdownCellContent = params.singleSelectDropdown;
        const dropdownValues = params.dropdownValues;

        console.log('Dropdown Values:', JSON.stringify(dropdownValues));
        this.choices.setChoices(
            dropdownValues.map((item) => {
                return {
                    value: item.value.toString(),
                    label: item.label,
                    selected: item.value == singleSelectDropdown.dropdownRowOid
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