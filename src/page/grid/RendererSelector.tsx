import { CellRendererSelectorResult } from 'ag-grid-community';
import { CellContent } from "../api/model/cell";
import { PlainTextCellRenderer } from './renderer/PlainTextCellRenderer';
import { CheckboxCellRenderer } from './renderer/CheckboxCellRenderer';
import { DateCellRenderer } from './renderer/DateCellRenderer';
import { DatetimeCellRenderer } from './renderer/DatetimeCellRenderer';
import { ObjectCellRenderer } from './renderer/ObjectCellRenderer';
import { ObjectPageBreadcrumb, SchemaPageBreadcrumb } from '../breadcrumb';
import { SchemaCellRenderer } from './renderer/SchemaCellRenderer';

export function selectRenderer(content: CellContent, onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void, onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,): CellRendererSelectorResult | undefined {
    if ('textEntry' in content) {
        switch (content.textEntry.format) {
            default: 
                return {
                    component: PlainTextCellRenderer,
                    params: {
                        deferRender: true,
                    }
                };
        }
    } else if ('checkboxEntry' in content) {
        return {
            component: CheckboxCellRenderer,
            params: {
                deferRender: true
            }
        };
    } else if ('dateEntry' in content) {
        return {
            component: DateCellRenderer,
            params: {
                deferRender: true
            }
        };
    } else if ('datetimeEntry' in content) {
        return {
            component: DatetimeCellRenderer,
            params: {
                deferRender: true
            }
        };
    } else if ('objectLink' in content) {
        return {
            component: ObjectCellRenderer,
            params: {
                deferRender: true,
                onRequestOpenObject
            }
        };
    } else if ('schemaLink' in content) {
        return {
            component: SchemaCellRenderer,
            params: {
                deferRender: true,
                onRequestOpenSchema
            }
        };
    }

    return undefined;
}