import { CellContent } from "../api/model/cell";
import { PageBreadcrumb } from "../breadcrumb";

export type FileData = {
    label: string
};

export type BreadcrumbData = {
    label: string,
    breadcrumb: PageBreadcrumb | null
};

export function getValue(content: CellContent): string | number | boolean | Date | FileData | BreadcrumbData | null {
    if ('textEntry' in content) {
        return content.textEntry.label;
    } else if ('integerEntry' in content) {
        return content.integerEntry.value;
    } else if ('numberEntry' in content) {
        return content.numberEntry.value;
    } else if ('checkboxEntry' in content) {
        return content.checkboxEntry.isChecked;
    } else if ('dateEntry' in content) {
        return content.dateEntry.label ? new Date(content.dateEntry.label) : null;
    } else if ('datetimeEntry' in content) {
        return content.datetimeEntry.label ? new Date(content.datetimeEntry.label) : null;
    } else if ('fileEntry' in content) {
        return {
            label: content.fileEntry.label ?? ''
        };
    } else if ('imageEntry' in content) {
        return {
            label: content.imageEntry.label ?? ''
        };
    } else if ('objectLink' in content) {
        return {
            label: content.objectLink.label ?? '',
            breadcrumb: {
                object: {}
            }
        };
    } else if ('schemaLink' in content) {
        
    } else if ('singleSelectDropdown' in content) {
        return content.singleSelectDropdown.dropdownRowOid?.toString() ?? null;
    } else if ('multiSelectDropdown' in content) {
        return content.multiSelectDropdown.dropdownRowOid.join(',');
    } else {
        return content.readonly.label;
    }
}