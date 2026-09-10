import { AgPromise, IHeaderParams, IInnerHeaderComponent } from 'ag-grid-community';
import { FullMetadata as ColumnFullMetadata } from '../../api/model/column';
import { Menu } from '@tauri-apps/api/menu';
import { executeAsync } from '../../api/action';
import classNames from 'classnames';

type ColumnHeaderParams = IHeaderParams & {
    columnMetadata: ColumnFullMetadata,
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata) => void,
    onError: (e: unknown) => void,
};

export class ColumnHeaderRenderer implements IInnerHeaderComponent {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
    }

    init(params: ColumnHeaderParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ColumnHeaderParams): boolean {
        const div = document.createElement('div');
        div.className = classNames('size-full', 'content-center');
        div.innerText = `${params.columnMetadata.isPrimaryKey ? '🔑 ' : ''}${params.columnMetadata.name}`;
        div.oncontextmenu = async () => {
            const menu = await Menu.new({
                items: [
                    {
                        text: "Edit",
                        action: () => {
                            params.onRequestEditColumn(params.columnMetadata);
                        }
                    },
                    {
                        text: "Delete",
                        action: async () => {
                            try {
                                await executeAsync({
                                    trashColumn: {
                                        schemaOid: params.columnMetadata.schema.oid,
                                        columnOid: params.columnMetadata.oid,
                                    }
                                });
                            } catch (e) {
                                params.onError(e);
                            }
                        }
                    }
                ]
            });
            menu.popup();
        };
        this.gui.replaceWith(div);
        this.gui = div;
        return true;
    }
}