import { ICellEditorComp, ICellEditorParams, ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import Choices, { EventChoice, InputChoice } from 'choices.js';
import { DropdownValue, queryAsync, SelectedHierarchicalListItemMetadata } from '../../api/query';
import classNames from 'classnames';
import { Channel } from '@tauri-apps/api/core';

export class SubtypeEditor implements ICellEditorComp {
    private gui: HTMLDivElement;
    private choices: Choices;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('size-full');
        const select = document.createElement('select');
        this.gui.appendChild(select);

        this.choices = new Choices(select, {
            choices: [],
            itemSelectText: '',
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

    init(params: ICellEditorParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    getValue() {
        const value = this.choices?.getValue();
        if (Array.isArray(value)) {
            return value.length > 0 ? value[0].value : null;
        } else if (value) {
            return value.value;
        } else {
            return null;
        }
    }

    refresh(params: ICellEditorParams): boolean {
        this.choices.setChoices(
            (params.data.inheritorTables as DropdownValue[])
                .map(({ value, label }) => { 
                    return { 
                        value: value.toString(), 
                        label,
                        selected: value == params.data.subtypeTableOid
                    }; 
                }),
            'value',
            'label',
            true
        );
        //this.choices.setValue([params.data.subtypeTableOid.toString()]);
        this.choices.showDropdown(false);
        return true;
    }
}