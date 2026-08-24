import { CellContent } from "./api/model/cell";

type CellProps = {
    content: CellContent,
    isFocused: boolean,
};

export function Cell(props: CellProps): React.JSX.Element {
    if ('textEntry' in props.content) {
        // Text cell
    } else if ('integerEntry' in props.content) {
        // Integer cell
    } else if ('numberEntry' in props.content) {
        // Number cell
    } else if ('dateEntry' in props.content) {
        // Date cell
    } else if ('datetimeEntry' in props.content) {
        // Datetime cell
    } else if ('checkboxEntry' in props.content) {
        // Checkbox cell
    } else if ('fileEntry' in props.content) {
        // File cell
    } else if ('imageEntry' in props.content) {
        // Image cell
    } else if ('schemaLink' in props.content) {
        // Schema link cell
    } else if ('objectLink' in props.content) {
        // Object link cell
    } else if ('singleSelectDropdown' in props.content) {
        // Single-Select Dropdown cell
    } else if ('multiSelectDropdown' in props.content) {
        // Multi-Select Dropdown cell
    } else {
        // Readonly cell
    }
    return (<td>

    </td>);
}