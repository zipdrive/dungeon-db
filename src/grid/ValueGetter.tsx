import { CellContent, FileEntryCellContent, ImageEntryCellContent, ObjectLinkCellContent, SchemaLinkCellContent } from "../api/model/cell";
import { getColumnAsync, getSchemaMetadataAsync } from "../api/query";
import { PageBreadcrumb } from "../breadcrumb";

export type FileData = {
    label: string
};

export type BreadcrumbData = {
    label: string,
    breadcrumb: PageBreadcrumb
};

export function getValue(content: CellContent): string | number | boolean | Date | FileEntryCellContent | ImageEntryCellContent | ObjectLinkCellContent | SchemaLinkCellContent | null {
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
        return content.fileEntry;
    } else if ('imageEntry' in content) {
        return content.imageEntry;
    } else if ('objectLink' in content) {
        return content.objectLink;
    } else if ('schemaLink' in content) {
        return content.schemaLink;
    } else if ('singleSelectDropdown' in content) {
        return content.singleSelectDropdown.label;
    } else if ('multiSelectDropdown' in content) {
        return content.multiSelectDropdown.label;
    } else {
        return content.readonly.label;
    }
}