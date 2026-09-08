import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { ObjectPageBreadcrumb } from '../../breadcrumb';
import { ObjectLinkCellContent } from '../../api/model/cell';
import { Schema } from '../../api/model/schema';
import { getColumnAsync, getSchemaMetadataAsync } from '../../api/query';

type ObjectCellRendererParams = ICellRendererParams<any, any, any> & {
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
};

export class ObjectCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center');
    }

    init(params: ObjectCellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ObjectCellRendererParams): boolean {
        const onRequestOpenObject: (object: ObjectPageBreadcrumb) => void = params.onRequestOpenObject;

        // Clear prior children
        for (const child of this.gui.childNodes) {
            this.gui.removeChild(child);
        }
        
        // 
        const link: HTMLAnchorElement = document.createElement('a');
        link.className = classNames(
            'text-[rgb(var(--color-primary)/1)]',
            'cursor-pointer'
        );
        this.gui.appendChild(link);

        const content: ObjectLinkCellContent | null = params.getValue ? params.getValue() : null;
        link.innerText = content?.label ?? '';
        link.onclick = async (_e) => {
            if (content) {
                if (content.linkRowOid) {
                    const schema = await getSchemaMetadataAsync(content.linkSchemaOid);
                    const columnMetadata = await getColumnAsync(content.cellIdentifier.columnOid);
                    onRequestOpenObject({
                        schema,
                        name: columnMetadata.name,
                        oidFilters: [['OID', content.linkRowOid]]
                    });
                } else {
                    
                }
            }
        };
        return true;
    }
}