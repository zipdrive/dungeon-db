import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { ObjectPageBreadcrumb } from '../../../breadcrumb';
import { ObjectTableCellContent } from '../../../api/model/tableRow';
import { getTableColumnMetadataAsync } from '../../../api/query';

type ObjectCellRendererParams = ICellRendererParams<any, any, any> & {
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export class ObjectCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center', 'h-full');
    }

    init(params: ObjectCellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ObjectCellRendererParams): boolean {
        const onRequestOpenObject: (object: ObjectPageBreadcrumb) => void = params.onRequestOpenObject;
        const onError: (e: unknown) => void = params.onError;

        try {
            // Clear prior children
            for (const child of this.gui.childNodes) {
                this.gui.removeChild(child);
            }
            
            // 
            const link: HTMLAnchorElement = document.createElement('a');
            link.className = classNames(
                'text-[rgb(var(--color-primary)/1)]',
                'absolute',
                'left-0',
                'right-0',
                'top-0',
                'bottom-0',
                'cursor-pointer'
            );
            this.gui.appendChild(link);

            const content: ObjectTableCellContent | null = params.getValue ? params.getValue() : null;
            link.innerText = content?.label ?? '';
            link.onclick = async (_e) => {
                if (content) {
                    if (content.value) {
                        onRequestOpenObject({
                            key: 'object',
                            name: '',
                            tableOid: content.tableOid,
                            rowOid: content.value
                        });
                    } else {
                        await executeAsync({
                            editCellContents: {
                                tableOid: content.dataTableOid,
                                columnOid: content.dataColumnOid,
                                rowOid: content.dataRowOid,
                                value: {
                                    object: {
                                        linkedRowOid: 'new'
                                    }
                                }
                            }
                        });
                    }
                }
            };
            return true;
        } catch (e) {
            onError(e);
            return false;
        }
    }
}