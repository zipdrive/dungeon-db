import { AgPromise, IHeaderParams, IInnerHeaderComponent } from 'ag-grid-community';
import { Menu } from '@tauri-apps/api/menu';
import classNames from 'classnames';
import { executeAsync } from '../../../api/action';
import { TableColumnMetadata } from '../../../api/model/tableColumn';

type ColumnHeaderParams = IHeaderParams & {
    columnMetadata: TableColumnMetadata,
    onRequestEditColumn: (columnMetadata: TableColumnMetadata) => void,
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
                                    trashTableColumn: {
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