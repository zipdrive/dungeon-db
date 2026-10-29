import { ICellEditorComp, ICellEditorParams, ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import Choices, { EventChoice, InputChoice } from 'choices.js';
import classNames from 'classnames';
import { Channel } from '@tauri-apps/api/core';
import { SingleSelectDropdownTableCellContent, TableRowLabel } from '../../../../api/model/tableRow';

type SingleSelectDropdownCellParams = ICellEditorParams & {
    singleSelectDropdown: SingleSelectDropdownTableCellContent,
    dropdownValues: {[tableOid: number]: TableRowLabel[]},
    loadDropdownValues: (tableOid: number) => void,
    onError: (e: unknown) => void,
};

export class SingleSelectCellEditor implements ICellEditorComp {
    private gui: HTMLDivElement;
    private choices: Choices;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('size-full');
        const select = document.createElement('select');
        this.gui.appendChild(select);

        this.choices = new Choices(select, {
            choices: [],
            shouldSort: false,
            itemSelectText: '',
            resetScrollPosition: false,
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

    init(params: SingleSelectDropdownCellParams) {
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

    refresh(params: SingleSelectDropdownCellParams): boolean {
        const { tableOid, value } = params.singleSelectDropdown;
        const dropdownValues = params.dropdownValues;
        if (tableOid in dropdownValues) {
            this.choices.setChoices(
                dropdownValues[tableOid].map((item) => {
                    return {
                        value: item.oid.toString(),
                        label: item.label ? item.label : ' ',
                        selected: item.oid == value
                    };
                }), 
                'value', 
                'label', 
                true
            );
        } else {
            this.choices.setChoices([]);
            params.loadDropdownValues(tableOid);
        }
        
        this.choices.showDropdown(false);
        return true;
    }
}