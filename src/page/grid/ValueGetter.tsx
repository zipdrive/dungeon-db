import { File } from "../../api/model/file";
import { MultiSelectDropdownTableCellContent, ObjectTableCellContent, SingleSelectDropdownTableCellContent, SubreportTableCellContent, TableCell, TableCellContent } from "../../api/model/tableRow";
import { PageBreadcrumb } from "../../breadcrumb";

export type FileData = {
    label: string
};

export type BreadcrumbData = {
    label: string,
    breadcrumb: PageBreadcrumb
};

export function getValue(cell: TableCell): string | number | boolean | Date | File | TableCell | null {
    if ('text' in cell.content) {
        return cell.content.text.value;
    } else if ('integer' in cell.content) {
        return cell.content.integer.value;
    } else if ('number' in cell.content) {
        return cell.content.number.value;
    } else if ('boolean' in cell.content) {
        return cell.content.boolean.value;
    } else if ('date' in cell.content) {
        return cell.content.date.label ? new Date(cell.content.date.label) : null;
    } else if ('datetime' in cell.content) {
        return cell.content.datetime.label ? new Date(cell.content.datetime.label) : null;
    } else if ('file' in cell.content) {
        return cell.content.file.value;
    } else {
        return cell;
    }
}