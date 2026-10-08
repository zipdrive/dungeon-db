import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { SchemaPageBreadcrumb } from '../../breadcrumb';
import { SchemaLinkCellContent } from '../../api/model/cell';
import { Schema } from '../../api/model/schema';
import { getColumnAsync, getSchemaMetadataAsync } from '../../api/query';
import { executeAsync } from '../../api/action';

type SchemaCellRendererParams = ICellRendererParams<any, any, any> & {
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export class SchemaCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center', 'h-full');
    }

    init(params: SchemaCellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: SchemaCellRendererParams): boolean {
        const onRequestOpenSchema = params.onRequestOpenSchema;
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
                'cursor-pointer',
                'w-full',
                'h-full',
                'inline-block'
            );
            this.gui.appendChild(link);

            const content: SchemaLinkCellContent | null = params.getValue ? params.getValue() : null;
            link.innerText = content?.label ?? '';
            link.onclick = async (_e) => {
                if (content) {
                    const schema = await getSchemaMetadataAsync(content.linkSchemaOid);
                    const columnMetadata = await getColumnAsync(content.cellIdentifier.columnOid);
                    onRequestOpenSchema({
                        schema,
                        name: columnMetadata.name,
                        oidFilters: content.linkOidFilters,
                        customFilters: []
                    });
                }
            };
            return true;
        } catch (e) {
            onError(e);
            return false;
        }
    }
}