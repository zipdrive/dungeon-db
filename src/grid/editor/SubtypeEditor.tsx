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
            classNames: {
                containerOuter: ['choices', 'size-full', 'z-1'],
                containerInner: ['choices__inner', 'border-none!', 'bg-transparent!', 'z-1'],
                input: ['choices__input', 'z-1'],
                inputCloned: ['choices__input--cloned', 'z-1'],
                list: ['choices__list', 'z-1'],
                listItems: ['choices__list--multiple', 'z-1'],
                listSingle: ['choices__list--single', 'z-1'],
                listDropdown: ['choices__list--dropdown', 'z-1'],
                item: ['choices__item', 'z-1'],
                itemSelectable: ['choices__item--selectable', 'z-1'],
                itemDisabled: ['choices__item--disabled', 'z-1'],
                itemChoice: ['choices__item--choice', 'z-1'],
                description: ['choices__description', 'z-1'],
                placeholder: ['choices__placeholder', 'z-1'],
                group: ['choices__group', 'z-1'],
                groupHeading: ['choices__heading', 'z-1'],
                button: ['choices__button', 'z-1'],
                activeState: ['is-active'],
                focusState: ['is-focused'],
                openState: ['is-open'],
                disabledState: ['is-disabled'],
                highlightedState: ['is-highlighted'],
                selectedState: ['is-selected'],
                flippedState: ['is-flipped'],
                loadingState: ['is-loading'],
                invalidState: ['is-invalid'],
                notice: ['choices__notice', 'z-1'],
                addChoice: ['choices__item--selectable', 'add-choice'],
                noResults: ['has-no-results', 'z-1'],
                noChoices: ['has-no-choices', 'z-1'],
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