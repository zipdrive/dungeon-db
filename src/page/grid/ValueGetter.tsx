import { File } from "../../api/model/file";
import { MultiSelectDropdownTableCellContent, ObjectTableCellContent, SingleSelectDropdownTableCellContent, SubreportTableCellContent, TableCellContent } from "../../api/model/tableRow";
import { PageBreadcrumb } from "../../breadcrumb";

export type FileData = {
    label: string
};

export type BreadcrumbData = {
    label: string,
    breadcrumb: PageBreadcrumb
};

export function getValue(content: TableCellContent): string | number | boolean | Date | File | ObjectTableCellContent | SingleSelectDropdownTableCellContent | MultiSelectDropdownTableCellContent | SubreportTableCellContent | null {
    if ('text' in content) {
        return content.text.value;
    } else if ('integer' in content) {
        return content.integer.value;
    } else if ('number' in content) {
        return content.number.value;
    } else if ('boolean' in content) {
        return content.boolean.value;
    } else if ('date' in content) {
        return content.date.label ? new Date(content.date.label) : null;
    } else if ('datetime' in content) {
        return content.datetime.label ? new Date(content.datetime.label) : null;
    } else if ('file' in content) {
        return content.file.value;
    } else if ('object' in content) {
        return content.object;
    } else if ('singleSelectDropdown' in content) {
        return content.singleSelectDropdown;
    } else if ('multiSelectDropdown' in content) {
        return content.multiSelectDropdown;
    } else if ('subreport' in content) {
        return content.subreport;
    } else {
        return null;
    }
}