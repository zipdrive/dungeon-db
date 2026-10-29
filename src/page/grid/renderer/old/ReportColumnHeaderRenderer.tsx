import { AgPromise, IHeaderParams, IInnerHeaderComponent } from 'ag-grid-community';
import { Menu } from '@tauri-apps/api/menu';
import classNames from 'classnames';
import { executeAsync } from '../../../../api/action';
import { ReportColumnMetadata } from '../../../../api/model/reportColumn';

type ReportColumnHeaderParams = IHeaderParams & {
    reportOid: number,
    columnMetadata: ReportColumnMetadata,
    columnList: ReportColumnMetadata[],
    onRequestCreateReportColumn: (tableOid: number, columnOid: number | null) => void,
    onRequestEditReportColumn: (columnMetadata: ReportColumnMetadata) => void,
    onError: (e: unknown) => void,
};

export class ReportColumnHeaderRenderer implements IInnerHeaderComponent {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
    }

    init(params: ReportColumnHeaderParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ReportColumnHeaderParams): boolean {
        const div = document.createElement('div');
        div.className = classNames('size-full', 'content-center');
        div.innerText = `${params.columnMetadata.isPrimaryKey ? '🔑 ' : ''}${params.columnMetadata.name}`;
        div.oncontextmenu = async () => {
            const menu = await Menu.new({
                items: [
                    {
                        text: "Insert Column Before",
                        action: () => {
                            params.onRequestCreateReportColumn(params.reportOid, params.columnMetadata.oid);
                        }
                    },
                    {
                        text: "Edit",
                        action: () => {
                            params.onRequestEditReportColumn(params.columnMetadata);
                        }
                    },
                    {
                        text: "Delete",
                        action: async () => {
                            try {
                                await executeAsync({
                                    trashReportColumn: {
                                        reportOid: params.reportOid,
                                        columnOid: params.columnMetadata.oid,
                                    }
                                });
                            } catch (e) {
                                params.onError(e);
                            }
                        }
                    },
                    {
                        text: "Move Before...",
                        items: params.columnList.map((item) => {
                            return {
                                text: item.name,
                                enabled: item.oid == params.columnMetadata.oid,
                                action: async () => {
                                    try {
                                        await executeAsync({
                                            moveReportColumnBefore: {
                                                metadata: params.columnMetadata,
                                                beforeColumnOid: item.oid
                                            }
                                        });
                                    } catch (e) {
                                        params.onError(e);
                                    }
                                }
                            }
                        })
                    },
                    {
                        text: "Move After...",
                        items: params.columnList.map((item, idx) => {
                            const next = idx < params.columnList.length - 1 ? params.columnList[idx + 1] : null;
                            return {
                                text: item.name,
                                action: async () => {
                                    try {
                                        await executeAsync({
                                            moveReportColumnBefore: {
                                                metadata: params.columnMetadata,
                                                beforeColumnOid: next?.oid || null
                                            }
                                        });
                                    } catch (e) {
                                        params.onError(e);
                                    }
                                }
                            }
                        })
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